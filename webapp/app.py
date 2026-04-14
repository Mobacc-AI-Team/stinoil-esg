from __future__ import annotations

import os
import re
from dataclasses import dataclass
from functools import lru_cache
from html import escape
from pathlib import Path
from typing import Iterable

from flask import Flask, abort, render_template, request


DEFAULT_KB_ROOT = Path(__file__).resolve().parent.parent / "ChemieComplianceKennisbank"
FRONTMATTER_DELIMITER = "---"
SECTION_MAP = {
    "wetgeving": "01_Wetgeving",
    "vragen": "02_Vragen",
    "beoordelingen": "03_Beoordelingen",
    "antwoorden": "04_Antwoorden",
    "bronnen": "05_Bronnen",
    "locaties": "06_Locaties",
    "templates": "07_Templates",
    "index": "08_Index",
    "workflows": "09_Workflows",
}


@dataclass
class DocumentRecord:
    title: str
    path: Path
    rel_path: str
    section_key: str
    section_label: str
    metadata: dict[str, str]
    body: str

    @property
    def search_blob(self) -> str:
        meta_text = " ".join(f"{key} {value}" for key, value in self.metadata.items())
        return f"{self.title} {meta_text} {self.body}".lower()

    @property
    def tags(self) -> list[str]:
        raw = self.metadata.get("tags", "")
        if raw.startswith("[") and raw.endswith("]"):
            inner = raw[1:-1].strip()
            if not inner:
                return []
            return [clean_tag(part) for part in inner.split(",") if clean_tag(part)]
        tag = clean_tag(raw)
        return [tag] if tag else []


@dataclass
class DashboardStats:
    wetgeving: int
    vragen: int
    beoordelingen: int
    antwoorden: int
    locaties: int


@dataclass
class SearchFilters:
    query: str
    section: str
    tag: str
    location: str
    status: str


def create_app() -> Flask:
    app = Flask(__name__)
    kb_root = Path(os.environ.get("KB_ROOT", DEFAULT_KB_ROOT)).expanduser().resolve()
    app.config["KB_ROOT"] = kb_root

    @app.context_processor
    def inject_globals() -> dict[str, object]:
        return {
            "kb_root": kb_root,
            "section_options": list(SECTION_MAP.items()),
        }

    @app.route("/")
    def dashboard() -> str:
        documents = load_documents(kb_root)
        stats = build_dashboard_stats(documents)
        recent_questions = [doc for doc in documents if doc.section_key == "vragen"][-8:]
        recent_answers = [doc for doc in documents if doc.section_key == "antwoorden"][-8:]
        return render_template(
            "dashboard.html",
            stats=stats,
            recent_questions=list(reversed(recent_questions)),
            recent_answers=list(reversed(recent_answers)),
            top_tags=top_tags(documents),
        )

    @app.route("/wetgeving")
    def wetgeving() -> str:
        query = request.args.get("q", "").strip()
        documents = [doc for doc in load_documents(kb_root) if doc.section_key == "wetgeving"]
        if query:
            needle = query.lower()
            documents = [doc for doc in documents if needle in doc.search_blob]
        return render_template("wetgeving.html", query=query, documents=documents)

    @app.route("/casussen")
    def casussen() -> str:
        filters = SearchFilters(
            query=request.args.get("q", "").strip(),
            section=request.args.get("section", "vragen").strip(),
            tag=request.args.get("tag", "").strip(),
            location=request.args.get("location", "").strip(),
            status=request.args.get("status", "").strip(),
        )
        docs = filter_case_documents(load_documents(kb_root), filters)
        return render_template(
            "casussen.html",
            filters=filters,
            documents=docs,
            available_tags=sorted(unique_values(load_documents(kb_root), "tags")),
            available_locations=sorted(unique_values(load_documents(kb_root), "locatie")),
            available_statuses=sorted(unique_values(load_documents(kb_root), "status")),
        )

    @app.route("/zoek")
    def zoek() -> str:
        filters = SearchFilters(
            query=request.args.get("q", "").strip(),
            section=request.args.get("section", "").strip(),
            tag=request.args.get("tag", "").strip(),
            location=request.args.get("location", "").strip(),
            status=request.args.get("status", "").strip(),
        )
        docs = filter_documents(load_documents(kb_root), filters)
        return render_template("search.html", filters=filters, documents=docs)

    @app.route("/document/<path:rel_path>")
    def document_detail(rel_path: str) -> str:
        safe_rel = Path(rel_path)
        target = (kb_root / safe_rel).resolve()
        if kb_root not in [target, *target.parents] or not target.is_file():
            abort(404)
        record = document_from_file(kb_root, target)
        return render_template(
            "document_detail.html",
            document=record,
            rendered_body=markdown_to_html(record.body),
        )

    return app


@lru_cache(maxsize=1)
def load_documents(kb_root: Path) -> tuple[DocumentRecord, ...]:
    if not kb_root.exists():
        return tuple()

    records: list[DocumentRecord] = []
    for path in sorted(kb_root.glob("**/*.md")):
        records.append(document_from_file(kb_root, path))
    return tuple(records)


def document_from_file(kb_root: Path, path: Path) -> DocumentRecord:
    metadata, body = split_frontmatter(path.read_text(encoding="utf-8"))
    rel_path = str(path.relative_to(kb_root)).replace("\\", "/")
    section_key = infer_section_key(rel_path)
    return DocumentRecord(
        title=metadata.get("titel") or path.stem.replace("_", " ").title(),
        path=path,
        rel_path=rel_path,
        section_key=section_key,
        section_label=section_label(section_key),
        metadata=metadata,
        body=body,
    )


def split_frontmatter(text: str) -> tuple[dict[str, str], str]:
    if not text.startswith(FRONTMATTER_DELIMITER):
        return {}, text

    lines = text.splitlines()
    metadata: dict[str, str] = {}
    end_index = 0
    for index, line in enumerate(lines[1:], start=1):
        if line.strip() == FRONTMATTER_DELIMITER:
            end_index = index
            break
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        metadata[key.strip()] = value.strip()

    body = "\n".join(lines[end_index + 1 :]).strip()
    return metadata, body


def infer_section_key(rel_path: str) -> str:
    top = rel_path.split("/", 1)[0]
    for key, folder in SECTION_MAP.items():
        if folder == top:
            return key
    return "overig"


def section_label(section_key: str) -> str:
    return {
        "wetgeving": "Wetgeving",
        "vragen": "Vragen",
        "beoordelingen": "Beoordelingen",
        "antwoorden": "Antwoorden",
        "bronnen": "Bronnen",
        "locaties": "Locaties",
        "templates": "Templates",
        "index": "Index",
        "workflows": "Workflows",
        "overig": "Overig",
    }.get(section_key, section_key.title())


def build_dashboard_stats(documents: Iterable[DocumentRecord]) -> DashboardStats:
    docs = list(documents)
    return DashboardStats(
        wetgeving=count_section(docs, "wetgeving"),
        vragen=count_section(docs, "vragen"),
        beoordelingen=count_section(docs, "beoordelingen"),
        antwoorden=count_section(docs, "antwoorden"),
        locaties=count_section(docs, "locaties"),
    )


def count_section(documents: Iterable[DocumentRecord], section_key: str) -> int:
    return sum(1 for doc in documents if doc.section_key == section_key)


def top_tags(documents: Iterable[DocumentRecord]) -> list[tuple[str, int]]:
    tag_counts: dict[str, int] = {}
    for doc in documents:
        for tag in doc.tags:
            tag_counts[tag] = tag_counts.get(tag, 0) + 1
    return sorted(tag_counts.items(), key=lambda item: (-item[1], item[0]))[:12]


def unique_values(documents: Iterable[DocumentRecord], field: str) -> set[str]:
    results: set[str] = set()
    for doc in documents:
        if field == "tags":
            results.update(doc.tags)
            continue
        value = doc.metadata.get(field, "").strip().strip('"')
        if value:
            results.add(value)
    return results


def filter_documents(documents: Iterable[DocumentRecord], filters: SearchFilters) -> list[DocumentRecord]:
    results = list(documents)
    if filters.section:
        results = [doc for doc in results if doc.section_key == filters.section]
    if filters.query:
        needle = filters.query.lower()
        results = [doc for doc in results if needle in doc.search_blob]
    if filters.tag:
        results = [doc for doc in results if filters.tag in doc.tags]
    if filters.location:
        results = [doc for doc in results if doc.metadata.get("locatie", "").strip('"') == filters.location]
    if filters.status:
        results = [doc for doc in results if doc.metadata.get("status", "").strip('"') == filters.status]
    return results


def filter_case_documents(documents: Iterable[DocumentRecord], filters: SearchFilters) -> list[DocumentRecord]:
    case_sections = {"vragen", "beoordelingen", "antwoorden"}
    docs = [doc for doc in documents if doc.section_key in case_sections]
    if filters.section and filters.section in case_sections:
        docs = [doc for doc in docs if doc.section_key == filters.section]
    if filters.query:
        needle = filters.query.lower()
        docs = [doc for doc in docs if needle in doc.search_blob]
    if filters.tag:
        docs = [doc for doc in docs if filters.tag in doc.tags]
    if filters.location:
        docs = [doc for doc in docs if doc.metadata.get("locatie", "").strip('"') == filters.location]
    if filters.status:
        docs = [doc for doc in docs if doc.metadata.get("status", "").strip('"') == filters.status]
    return docs


def clean_tag(value: str) -> str:
    return value.strip().strip('"').strip("'")


def markdown_to_html(markdown_text: str) -> str:
    blocks = markdown_text.split("\n\n")
    html_blocks: list[str] = []
    for block in blocks:
        stripped = block.strip()
        if not stripped:
            continue
        lines = stripped.splitlines()
        if all(line.lstrip().startswith("- ") for line in lines):
            items = "".join(f"<li>{inline_markdown(line.lstrip()[2:])}</li>" for line in lines)
            html_blocks.append(f"<ul>{items}</ul>")
            continue
        if stripped.startswith("### "):
            html_blocks.append(f"<h3>{inline_markdown(stripped[4:])}</h3>")
            continue
        if stripped.startswith("## "):
            html_blocks.append(f"<h2>{inline_markdown(stripped[3:])}</h2>")
            continue
        if stripped.startswith("# "):
            html_blocks.append(f"<h1>{inline_markdown(stripped[2:])}</h1>")
            continue
        html_blocks.append("".join(f"<p>{inline_markdown(line)}</p>" for line in lines))
    return "\n".join(html_blocks)


def inline_markdown(text: str) -> str:
    text = escape(text)
    text = re.sub(r"`([^`]+)`", r"<code>\1</code>", text)
    text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r'<a href="/document/\2">\1</a>', text)
    return text


app = create_app()


if __name__ == "__main__":
    app.run(debug=True)
