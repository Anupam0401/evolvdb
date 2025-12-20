#!/usr/bin/env python3
import argparse
import os
import re
import shlex
import subprocess
import sys
from pathlib import Path


def run_cmd(cmd, cwd):
    p = subprocess.run(cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if p.returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(shlex.quote(c) for c in cmd)}\n{p.stderr}")
    return p.stdout


def detect_upstream_base(repo_root: Path) -> str:
    """
    Detect the base branch for PR comparison.
    For feature branches, this should be origin/master (the PR target).
    Returns origin/master if available, otherwise tries master, then HEAD.
    """
    # Get current branch name
    current_branch = None
    try:
        p = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            cwd=repo_root,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if p.returncode == 0:
            current_branch = p.stdout.strip()
    except Exception:
        pass
    
    # If on master/main branch, compare against its upstream tracking
    if current_branch in ["master", "main"]:
        try:
            p = subprocess.run(
                ["git", "rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}"],
                cwd=repo_root,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            if p.returncode == 0 and p.stdout.strip():
                return p.stdout.strip()
        except Exception:
            pass
    
    # For feature branches, compare against origin/master (PR target)
    # Check if origin/master exists
    try:
        p = subprocess.run(
            ["git", "rev-parse", "--verify", "origin/master"],
            cwd=repo_root,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if p.returncode == 0:
            return "origin/master"
    except Exception:
        pass
    
    # Try origin/main as fallback
    try:
        p = subprocess.run(
            ["git", "rev-parse", "--verify", "origin/main"],
            cwd=repo_root,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if p.returncode == 0:
            return "origin/main"
    except Exception:
        pass
    
    # Try local master branch
    try:
        p = subprocess.run(
            ["git", "rev-parse", "--verify", "master"],
            cwd=repo_root,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if p.returncode == 0:
            return "master"
    except Exception:
        pass
    
    # Try local main branch
    try:
        p = subprocess.run(
            ["git", "rev-parse", "--verify", "main"],
            cwd=repo_root,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if p.returncode == 0:
            return "main"
    except Exception:
        pass
    
    # If nothing works, fall back to HEAD
    return "HEAD"


def get_repo_root(cwd):
    out = run_cmd(["git", "rev-parse", "--show-toplevel"], cwd)
    return Path(out.strip())


def detect_binary(path: Path, sample_bytes: int = 2048) -> bool:
    try:
        with open(path, "rb") as f:
            chunk = f.read(sample_bytes)
        if b"\x00" in chunk:
            return True
        if not chunk:
            return False
        textish = sum(1 for b in chunk if 9 <= b <= 13 or 32 <= b <= 126)
        return (textish / max(1, len(chunk))) < 0.8
    except Exception:
        return True


def collect_diff(repo_root: Path, base: str, mode: str, include_untracked: bool) -> str:
    args = ["git", "diff", "--patch", "--unified=3"]
    if mode == "staged":
        args.append("--staged")
    if base:
        args.append(base)
    text = run_cmd(args, repo_root)

    if include_untracked:
        untracked = run_cmd(["git", "ls-files", "--others", "--exclude-standard"], repo_root)
        files = [line.strip() for line in untracked.splitlines() if line.strip()]
        if files:
            snippets = []
            for rel in files:
                p = repo_root / rel
                if not p.is_file():
                    continue
                if detect_binary(p):
                    continue
                try:
                    u = run_cmd(["git", "diff", "--patch", "--unified=3", "--no-index", "--", "/dev/null", str(p)], repo_root)
                    # Normalize paths in header to look like a regular diff
                    u = re.sub(r"^diff --git a/(/dev/null) b/(.*)$", lambda m: f"diff --git a/{m.group(1)} b/{os.path.relpath(p, repo_root)}", u, flags=re.M)
                    u = re.sub(r"^\+\+\+ b/.*$", f"+++ b/{os.path.relpath(p, repo_root)}", u, flags=re.M)
                    u = re.sub(r"^--- a/.*$", f"--- a/{os.path.relpath(p, repo_root)}", u, flags=re.M)
                    snippets.append(u)
                except Exception:
                    continue
            if snippets:
                text = text + ("\n" if text and not text.endswith("\n") else "") + "\n".join(snippets)
    return text


_SECRET_LINE_RE = re.compile(r"(api[_-]?key|secret|token|password)", re.I)
_HEXLIKE_RE = re.compile(r"[A-Za-z0-9_\-]{24,}")


def redact_secrets(diff_text: str) -> str:
    # redact private key blocks
    diff_text = re.sub(
        r"-----BEGIN [^-]+-----[\s\S]*?-----END [^-]+-----",
        "-----BEGIN REDACTED-----\n[REDACTED PRIVATE KEY]\n-----END REDACTED-----",
        diff_text,
        flags=re.M,
    )

    # redact suspicious tokens on sensitive lines
    redacted_lines = []
    for line in diff_text.splitlines():
        if line.startswith("+") or line.startswith("-") or line.startswith(" "):
            content = line[1:]
            if _SECRET_LINE_RE.search(content):
                content = _HEXLIKE_RE.sub("[REDACTED]", content)
                line = line[0] + content
        redacted_lines.append(line)
    return "\n".join(redacted_lines) + ("\n" if diff_text.endswith("\n") else "")


def chunk_text_by_header(text: str, max_bytes: int):
    if len(text.encode("utf-8")) <= max_bytes:
        return [text]
    parts = re.split(r"(?=^diff --git \\S+ \\S+$)", text, flags=re.M)
    chunks = []
    cur = []
    cur_size = 0
    for part in parts:
        if not part:
            continue
        b = part.encode("utf-8")
        if not cur:
            cur.append(part)
            cur_size = len(b)
            continue
        if cur_size + len(b) <= max_bytes:
            cur.append(part)
            cur_size += len(b)
        else:
            chunks.append("".join(cur))
            cur = [part]
            cur_size = len(b)
    if cur:
        chunks.append("".join(cur))
    return chunks


def main():
    parser = argparse.ArgumentParser(prog="collect_diff", add_help=True)
    parser.add_argument(
        "--base", 
        default="auto",
        help="Diff base (e.g., HEAD, origin/master). Use 'auto' to detect upstream branch (default: auto)"
    )
    parser.add_argument("--mode", choices=["worktree", "staged"], default="worktree")
    parser.add_argument("--include-untracked", action="store_true")
    parser.add_argument("--no-redact", action="store_true")
    parser.add_argument("--max-bytes", type=int, default=50_000_000)  # 50MB - support very large PRs
    parser.add_argument("--output", choices=["stdout", "file"], default="stdout")
    parser.add_argument("--out", default="auto_review.diff")

    args = parser.parse_args()

    cwd = os.getcwd()
    repo_root = get_repo_root(cwd)

    # Auto-detect upstream branch if 'auto' is specified
    base = args.base
    if base.lower() == "auto":
        base = detect_upstream_base(repo_root)
        print(f"Auto-detected base branch: {base}", file=sys.stderr)
    if args.mode == "worktree":
        diff_text = collect_diff(repo_root, base, mode="worktree", include_untracked=args.include_untracked)
    else:
        diff_text = collect_diff(repo_root, base, mode="staged", include_untracked=False)

    if not args.no_redact:
        diff_text = redact_secrets(diff_text)

    header = (
        f"# AUTO_REVIEW_DIFF\n"
        f"base: {base}\n"
        f"mode: {args.mode}\n"
        f"include_untracked: {bool(args.include_untracked)}\n"
        f"redacted: {not args.no_redact}\n"
    )

    if not diff_text.strip():
        out = header + "\n# No changes detected.\n"
        if args.output == "stdout":
            sys.stdout.write(out)
            return
        Path(args.out).write_text(out, encoding="utf-8")
        return

    chunks = chunk_text_by_header(diff_text, max_bytes=max(10_000, args.max_bytes))
    parts = []
    for i, chunk in enumerate(chunks, 1):
        parts.append(
            f"==== AUTO_REVIEW_DIFF CHUNK {i}/{len(chunks)} ====\n" + header + "```diff\n" + chunk + "\n```\n"
        )
    payload = "\n".join(parts)

    if args.output == "stdout":
        sys.stdout.write(payload)
    else:
        Path(args.out).write_text(payload, encoding="utf-8")


if __name__ == "__main__":
    main()
