from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app import create_app  # noqa: E402


@dataclass
class AuditIssue:
    code: str
    path: Path
    line: int
    message: str


HREF_HASH_RE = re.compile(r"href\s*=\s*['\"]#['\"]")
DISABLED_BUTTON_RE = re.compile(r"<button\b[^>]*\bdisabled\b", re.IGNORECASE)
CTA_PENDING_RE = re.compile(
    r"<(a|button)\b[^>]*>\s*([^<]*(todo|pendiente)[^<]*)\s*</\1>",
    re.IGNORECASE,
)
URL_FOR_RE = re.compile(r"url_for\(\s*['\"]([^'\"]+)['\"]")
DATA_ACTION_RE = re.compile(r"data-action\s*=\s*['\"]([^'\"]+)['\"]", re.IGNORECASE)


def line_no(text: str, index: int) -> int:
    return text.count("\n", 0, index) + 1


def discover_template_files() -> list[Path]:
    return sorted((ROOT / "app" / "templates").rglob("*.html"))


def discover_js_files() -> list[Path]:
    return sorted((ROOT / "app" / "static").rglob("*.js"))


def collect_handler_corpus(template_files: list[Path], js_files: list[Path]) -> str:
    chunks: list[str] = []
    script_re = re.compile(r"<script[^>]*>(.*?)</script>", re.IGNORECASE | re.DOTALL)
    for template in template_files:
        text = template.read_text(encoding="utf-8")
        chunks.extend(script_re.findall(text))
    for js_file in js_files:
        chunks.append(js_file.read_text(encoding="utf-8"))
    return "\n".join(chunks)


def audit_ui() -> list[AuditIssue]:
    issues: list[AuditIssue] = []
    template_files = discover_template_files()
    js_files = discover_js_files()
    handler_corpus = collect_handler_corpus(template_files, js_files)

    app = create_app()
    valid_endpoints = {rule.endpoint for rule in app.url_map.iter_rules()}

    for template in template_files:
        text = template.read_text(encoding="utf-8")

        for match in HREF_HASH_RE.finditer(text):
            issues.append(
                AuditIssue(
                    code="UI001",
                    path=template,
                    line=line_no(text, match.start()),
                    message='Dead link href="#"',
                )
            )

        for match in DISABLED_BUTTON_RE.finditer(text):
            issues.append(
                AuditIssue(
                    code="UI002",
                    path=template,
                    line=line_no(text, match.start()),
                    message="Disabled action button",
                )
            )

        for match in CTA_PENDING_RE.finditer(text):
            issues.append(
                AuditIssue(
                    code="UI003",
                    path=template,
                    line=line_no(text, match.start()),
                    message="CTA text contains TODO/pendiente",
                )
            )

        for match in URL_FOR_RE.finditer(text):
            endpoint = match.group(1)
            if endpoint not in valid_endpoints:
                issues.append(
                    AuditIssue(
                        code="UI005",
                        path=template,
                        line=line_no(text, match.start()),
                        message=f"url_for unresolved endpoint: {endpoint}",
                    )
                )

        for match in DATA_ACTION_RE.finditer(text):
            action_name = match.group(1)
            if action_name not in handler_corpus:
                issues.append(
                    AuditIssue(
                        code="UI004",
                        path=template,
                        line=line_no(text, match.start()),
                        message=f"data-action without handler: {action_name}",
                    )
                )

    issues.sort(key=lambda x: (str(x.path), x.line, x.code))
    return issues


def main() -> int:
    issues = audit_ui()
    if not issues:
        print("UI audit passed: no blocking issues found.")
        return 0

    print("UI audit found issues:")
    for item in issues:
        rel = item.path.relative_to(ROOT)
        print(f"- {item.code} {rel}:{item.line} {item.message}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
