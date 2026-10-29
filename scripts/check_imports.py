#!/usr/bin/env python3
"""check_imports.py - list the outside packages the SDG-6 pipeline imports
and flag any that requirements.txt does not cover.

Only the scripts the pipeline actually runs are checked: the modules named in
pipeline_config.yaml (`module: scripts.xxx` and `script: path.py`), any extra
entry scripts given with --entry, and the project files those import in turn. Old scripts in the folder
that nothing uses are ignored. Read-only; standard library only.

    python check_imports.py --root . --config config/pipeline_config.yaml \
        --entry scripts/BatchRunner.py --requirements requirements.txt
"""
import argparse
import ast
import re
import sys
from pathlib import Path

__version__ = "1.2.0"   # 1.2: also follows `script:` tool processes

# import name -> package name on PyPI, where they differ
IMPORT_TO_PACKAGE = {
    "yaml": "pyyaml",
    "PIL": "pillow",
    "dateutil": "python-dateutil",
    "sklearn": "scikit-learn",
    "cv2": "opencv-python",
    "fitz": "pymupdf",
    "docx": "python-docx",
}


def norm(name):
    return re.sub(r"[-_.]+", "-", name).lower()


# ---------------------------------------------------------------- entry points
def modules_from_config(config_path):
    """Module names from `module: scripts.xxx` lines (commented lines skipped)."""
    mods = []
    for line in Path(config_path).read_text(encoding="utf-8").splitlines():
        m = re.match(r"\s*module:\s*([\w.]+)", line)
        if m:
            mods.append(m.group(1))
    return mods


def scripts_from_config(config_path):
    """Script paths from `script: path/to/tool.py` lines (BatchRunner tool processes)."""
    paths = []
    for line in Path(config_path).read_text(encoding="utf-8").splitlines():
        m = re.match(r"\s*script:\s*(\S+\.py)", line)
        if m:
            paths.append(m.group(1))
    return paths


def module_to_file(root, module, from_dir=None):
    """Find the .py file for a module name: project root first, then the
    importing script's own folder (Python adds it to the path)."""
    rel = Path(*module.split("."))
    for base in filter(None, [root, from_dir]):
        for cand in (base / rel.with_suffix(".py"), base / rel / "__init__.py"):
            if cand.is_file():
                return cand.resolve()
    return None


def is_local_dir(root, name, from_dir=None):
    """A project folder used as a package without __init__.py (e.g. scripts/)."""
    return any((base / name).is_dir() for base in filter(None, [root, from_dir]))


# ---------------------------------------------------------------- import scan
def imports_in(path):
    """Top-level-qualified import names in one file (absolute imports only)."""
    tree = ast.parse(path.read_text(encoding="utf-8", errors="replace"))
    names = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names += [a.name for a in node.names]
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            names.append(node.module)
            names += [f"{node.module}.{a.name}" for a in node.names]  # from scripts import x
    return names


def follow(root, start_files):
    """Walk from the entry files through project-local imports.
    Returns (files used, {outside import: set of files})."""
    seen, outside, todo = set(), {}, list(start_files)
    stdlib = set(sys.stdlib_module_names)
    while todo:
        f = todo.pop()
        if f in seen:
            continue
        seen.add(f)
        for name in imports_in(f):
            local = module_to_file(root, name, f.parent)
            if local:
                todo.append(local)
                continue
            top = name.split(".")[0]
            if top in stdlib or module_to_file(root, top, f.parent) or is_local_dir(root, top, f.parent):
                continue
            if "." in name and name.rsplit(".", 1)[0] and module_to_file(root, name.rsplit(".", 1)[0], f.parent):
                continue  # `from scripts import x` where x is a function, not a file
            outside.setdefault(top, set()).add(str(f.relative_to(root)))
    return seen, outside


def read_requirements(path):
    pkgs = set()
    for line in Path(path).read_text().splitlines():
        line = line.split("#")[0].strip()
        if line:
            pkgs.add(norm(re.split(r"[=<>!~ ]", line)[0]))
    return pkgs


# ---------------------------------------------------------------- report
def report(root, used, outside, required, missing_entries):
    print(f"check_imports {__version__}\n")
    if missing_entries:
        print("Modules named in the config/entries but NOT FOUND:")
        for m in dict.fromkeys(missing_entries):
            print(f"  {m}")
        print()
    print(f"Project files the pipeline uses ({len(used)}):")
    for f in sorted(str(p.relative_to(root)) for p in used):
        print(f"  {f}")
    print("\nOutside packages:")
    missing = []
    for name in sorted(outside, key=str.lower):
        pkg = norm(IMPORT_TO_PACKAGE.get(name, name))
        ok = pkg in required
        if not ok:
            missing.append((pkg, sorted(outside[name])))
        print(f"  {'ok     ' if ok else 'MISSING'} {name} ({pkg})")
    if missing:
        print("\nNot in requirements.txt:")
        for pkg, files in missing:
            print(f"  {pkg}: used in " + ", ".join(files))
    else:
        print("\nAll outside packages are covered.")
    return 1 if (missing or missing_entries) else 0


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--root", default=".", help="project folder")
    ap.add_argument("--config", help="pipeline_config.yaml")
    ap.add_argument("--entry", nargs="*", default=[], help="extra scripts that are run directly")
    ap.add_argument("--requirements", default="requirements.txt")
    a = ap.parse_args()

    root = Path(a.root).resolve()
    starts, missing_entries = [], []
    for mod in (modules_from_config(a.config) if a.config else []):
        f = module_to_file(root, mod)
        (starts.append(f) if f else missing_entries.append(mod))
    for e in (scripts_from_config(a.config) if a.config else []) + a.entry:
        p = (root / e).resolve()
        (starts.append(p) if p.is_file() else missing_entries.append(e))

    used, outside = follow(root, starts)
    sys.exit(report(root, used, outside, read_requirements(a.requirements), missing_entries))


if __name__ == "__main__":
    main()
