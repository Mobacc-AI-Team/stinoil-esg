#!/usr/bin/env python3
"""
Zet een ChatGPT data-export om naar een centrale compliance-kennisbank
voor een chemisch bedrijf.

Doel:
- relevante wetgeving centraal beheren
- klant- en leveranciersvragen registreren
- vragen toetsen aan wetgeving en interne kaders
- uniforme antwoorden opslaan voor hergebruik over locaties heen

Ondersteunt:
- een export .zip met een conversations.json erin
- direct een conversations.json bestand

Output:
ChemieComplianceKennisbank/
  00_Inbox/
  01_Wetgeving/
  02_Vragen/<jaar>/...
  03_Beoordelingen/<jaar>/...
  04_Antwoorden/<jaar>/...
  05_Bronnen/
  06_Locaties/
  07_Templates/
  08_Index/
  09_Workflows/

Gebruik:
    python chatgpt_export_to_kb.py /pad/naar/export.zip
    python chatgpt_export_to_kb.py /pad/naar/conversations.json --output ./ChemieComplianceKennisbank

Optioneel:
    python chatgpt_export_to_kb.py export.zip --slug-prefix locatie_a
    python chatgpt_export_to_kb.py export.zip --limit 25
    python chatgpt_export_to_kb.py export.zip --overwrite
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import textwrap
import zipfile
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_ROOT = "ChemieComplianceKennisbank"
FRONTMATTER_DELIMITER = "---"


@dataclass
class MessageRecord:
    role: str
    author_name: str
    created_at: datetime | None
    text: str


@dataclass
class ConversationRecord:
    title: str
    create_time: datetime | None
    update_time: datetime | None
    messages: list[MessageRecord]
    raw: dict[str, Any]


@dataclass
class CasePaths:
    vraag: Path
    beoordeling: Path
    antwoord: Path


@dataclass
class CaseIndexRecord:
    title: str
    date: str
    year: str
    tags: list[str]
    vraag_rel: str
    beoordeling_rel: str
    antwoord_rel: str
    locatie: str
    afzender_type: str
    status: str


class ExportFormatError(RuntimeError):
    pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Zet ChatGPT export om naar een compliance-kennisbankstructuur."
    )
    parser.add_argument(
        "input_path",
        type=Path,
        help="Pad naar export.zip of conversations.json",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(DEFAULT_ROOT),
        help=f"Uitvoermap. Standaard: ./{DEFAULT_ROOT}",
    )
    parser.add_argument(
        "--slug-prefix",
        default="",
        help="Optionele prefix voor bestandsnamen, bijvoorbeeld 'locatie_a' of 'klantvraag'.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Verwerk maximaal N gesprekken.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Bestaande bestanden met dezelfde naam overschrijven.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    try:
        conversations = load_conversations(args.input_path)
    except Exception as exc:  # noqa: BLE001
        print(f"FOUT bij laden export: {exc}", file=sys.stderr)
        return 1

    if args.limit is not None:
        conversations = conversations[: args.limit]

    output_root = args.output.expanduser().resolve()
    prepare_structure(output_root)
    write_static_files(output_root)

    written_count = 0
    for conv in conversations:
        if write_case_bundle(
            conv,
            output_root=output_root,
            slug_prefix=args.slug_prefix.strip(),
            overwrite=args.overwrite,
        ):
            written_count += 1

    build_indexes(output_root)

    print(f"Klaar. Verwerkt: {written_count} casussen")
    print(f"Map: {output_root}")
    return 0


def load_conversations(input_path: Path) -> list[ConversationRecord]:
    input_path = input_path.expanduser().resolve()
    if not input_path.exists():
        raise FileNotFoundError(f"Bestand niet gevonden: {input_path}")

    if input_path.suffix.lower() == ".zip":
        payload = read_conversations_json_from_zip(input_path)
    elif input_path.name == "conversations.json" or input_path.suffix.lower() == ".json":
        payload = json.loads(input_path.read_text(encoding="utf-8"))
    else:
        raise ExportFormatError(
            "Verwacht een .zip export of een conversations.json bestand."
        )

    if not isinstance(payload, list):
        raise ExportFormatError("conversations.json bevat geen lijst met gesprekken.")

    parsed: list[ConversationRecord] = []
    for item in payload:
        if isinstance(item, dict):
            parsed.append(parse_conversation(item))

    parsed.sort(
        key=lambda c: c.update_time
        or c.create_time
        or datetime.min.replace(tzinfo=timezone.utc)
    )
    return parsed


def read_conversations_json_from_zip(zip_path: Path) -> Any:
    with zipfile.ZipFile(zip_path, "r") as archive:
        candidates = sorted(
            name for name in archive.namelist() if name.endswith("conversations.json")
        )
        if not candidates:
            raise ExportFormatError("Geen conversations.json gevonden in zip-export.")
        with archive.open(candidates[0], "r") as handle:
            return json.load(handle)


def parse_conversation(item: dict[str, Any]) -> ConversationRecord:
    return ConversationRecord(
        title=clean_title(item.get("title") or "ongespecificeerde casus"),
        create_time=parse_timestamp(item.get("create_time")),
        update_time=parse_timestamp(item.get("update_time")),
        messages=extract_messages(item),
        raw=item,
    )


def extract_messages(item: dict[str, Any]) -> list[MessageRecord]:
    mapping = item.get("mapping")
    if not isinstance(mapping, dict):
        return []

    records: list[MessageRecord] = []
    for node in mapping.values():
        if not isinstance(node, dict):
            continue
        message = node.get("message")
        if not isinstance(message, dict):
            continue

        author = message.get("author") or {}
        role = str(author.get("role") or "unknown").strip().lower()
        author_name = str(author.get("name") or "").strip()
        created_at = parse_timestamp(message.get("create_time"))
        text = extract_message_text(message)
        if not text.strip():
            continue

        records.append(
            MessageRecord(
                role=role,
                author_name=author_name,
                created_at=created_at,
                text=text.strip(),
            )
        )

    records.sort(
        key=lambda record: record.created_at or datetime.min.replace(tzinfo=timezone.utc)
    )
    return records


def extract_message_text(message: dict[str, Any]) -> str:
    content = message.get("content")
    if not isinstance(content, dict):
        return ""

    parts = content.get("parts")
    if isinstance(parts, list):
        chunks: list[str] = []
        for part in parts:
            if isinstance(part, str):
                chunks.append(part)
            elif isinstance(part, dict):
                text = part.get("text")
                if isinstance(text, str):
                    chunks.append(text)
        return "\n\n".join(chunk for chunk in chunks if chunk and chunk.strip())

    for key in ("text", "result"):
        value = content.get(key)
        if isinstance(value, str):
            return value

    return ""


def parse_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        except Exception:  # noqa: BLE001
            return None
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        try:
            if raw.endswith("Z"):
                raw = raw[:-1] + "+00:00"
            parsed = datetime.fromisoformat(raw)
        except Exception:  # noqa: BLE001
            return None
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    return None


def prepare_structure(root: Path) -> None:
    for rel in [
        "00_Inbox",
        "01_Wetgeving/eu",
        "01_Wetgeving/nationaal",
        "01_Wetgeving/lokaal",
        "01_Wetgeving/normen_en_richtlijnen",
        "02_Vragen",
        "03_Beoordelingen",
        "04_Antwoorden",
        "05_Bronnen/interne_documenten",
        "05_Bronnen/pdf",
        "05_Bronnen/spreadsheets",
        "05_Bronnen/emails",
        "06_Locaties",
        "07_Templates",
        "08_Index",
        "09_Workflows",
    ]:
        (root / rel).mkdir(parents=True, exist_ok=True)


def write_static_files(root: Path) -> None:
    static_files = {
        "README.md": README_TEXT,
        "AGENTS.md": AGENTS_TEXT,
        ".gitignore": GITIGNORE_TEXT,
        "01_Wetgeving/eu/reach_verordening.md": STARTDOC_REACH,
        "01_Wetgeving/eu/clp_verordening.md": STARTDOC_CLP,
        "01_Wetgeving/normen_en_richtlijnen/adr_transport_gevaarlijke_stoffen.md": STARTDOC_ADR,
        "01_Wetgeving/nationaal/pgs15_opslag_verpakte_gevaarlijke_stoffen.md": STARTDOC_PGS15,
        "01_Wetgeving/nationaal/brzo_seveso.md": STARTDOC_BRZO,
        "01_Wetgeving/nationaal/bal_activiteiten_leefomgeving.md": STARTDOC_BAL,
        "07_Templates/template_wetgeving.md": TEMPLATE_WETGEVING,
        "07_Templates/template_vraag.md": TEMPLATE_VRAAG,
        "07_Templates/template_beoordeling.md": TEMPLATE_BEOORDELING,
        "07_Templates/template_antwoord.md": TEMPLATE_ANTWOORD,
        "07_Templates/template_locatieprofiel.md": TEMPLATE_LOCATIEPROFIEL,
        "09_Workflows/import_procedure.md": IMPORT_PROCEDURE,
        "09_Workflows/codex_queries.md": CODEX_QUERIES,
    }
    for rel_path, content in static_files.items():
        target = root / rel_path
        if not target.exists():
            target.write_text(content.strip() + "\n", encoding="utf-8")


def write_case_bundle(
    conv: ConversationRecord,
    output_root: Path,
    slug_prefix: str,
    overwrite: bool,
) -> bool:
    basis_datum = conv.update_time or conv.create_time or datetime.now(tz=timezone.utc)
    year = str(basis_datum.year)
    date_str = basis_datum.strftime("%Y-%m-%d")
    slug = slugify(conv.title)
    if slug_prefix:
        slug = f"{slugify(slug_prefix)}_{slug}"

    vraag_dir = output_root / "02_Vragen" / year
    beoordeling_dir = output_root / "03_Beoordelingen" / year
    antwoord_dir = output_root / "04_Antwoorden" / year
    vraag_dir.mkdir(parents=True, exist_ok=True)
    beoordeling_dir.mkdir(parents=True, exist_ok=True)
    antwoord_dir.mkdir(parents=True, exist_ok=True)

    paths = resolve_case_paths(
        vraag_dir=vraag_dir,
        beoordeling_dir=beoordeling_dir,
        antwoord_dir=antwoord_dir,
        date_str=date_str,
        slug=slug,
        overwrite=overwrite,
    )

    paths.vraag.write_text(render_vraag_markdown(conv, output_root, paths), encoding="utf-8")
    paths.beoordeling.write_text(
        render_beoordeling_markdown(conv, output_root, paths),
        encoding="utf-8",
    )
    paths.antwoord.write_text(
        render_antwoord_markdown(conv, output_root, paths),
        encoding="utf-8",
    )
    return True


def resolve_case_paths(
    vraag_dir: Path,
    beoordeling_dir: Path,
    antwoord_dir: Path,
    date_str: str,
    slug: str,
    overwrite: bool,
) -> CasePaths:
    attempt = 0
    while True:
        suffix = "" if attempt == 0 else f"_{attempt + 1}"
        base_name = f"{date_str}_{slug}{suffix}"
        vraag_path = vraag_dir / f"{base_name}_vraag.md"
        beoordeling_path = beoordeling_dir / f"{base_name}_beoordeling.md"
        antwoord_path = antwoord_dir / f"{base_name}_antwoord.md"
        if overwrite or not any(path.exists() for path in (vraag_path, beoordeling_path, antwoord_path)):
            return CasePaths(vraag=vraag_path, beoordeling=beoordeling_path, antwoord=antwoord_path)
        attempt += 1


def render_vraag_markdown(conv: ConversationRecord, output_root: Path, paths: CasePaths) -> str:
    datum = format_case_date(conv)
    title = conv.title
    samenvatting = make_short_summary(conv)
    vraagtekst = first_message_text(conv, "user") or "Nog aan te vullen"
    locatie = infer_location(conv)
    afzender_type = infer_sender_type(conv)
    tags = format_yaml_list(derive_tags(conv))

    return textwrap.dedent(
        f"""
        ---
        titel: {yaml_escape(title)}
        datum: {datum}
        locatie: {yaml_escape(locatie)}
        afzender_type: {yaml_escape(afzender_type)}
        status: nieuw
        tags: {tags}
        beoordeling: {yaml_escape(relative_path(output_root, paths.beoordeling))}
        antwoord: {yaml_escape(relative_path(output_root, paths.antwoord))}
        samenvatting: {yaml_escape(samenvatting)}
        ---

        # Vraag

        ## Onderwerp
        {title}

        ## Herkomst
        - Type afzender: {afzender_type}
        - Locatie: {locatie}
        - Invoerbron: ChatGPT export

        ## Vraagstelling
        {vraagtekst}

        ## Volledige context
        {render_full_thread(conv)}
        """
    ).strip() + "\n"


def render_beoordeling_markdown(conv: ConversationRecord, output_root: Path, paths: CasePaths) -> str:
    datum = format_case_date(conv)
    title = conv.title
    samenvatting = make_short_summary(conv)
    tags = derive_tags(conv)
    relevante_wetgeving = suggest_regulations(tags)
    beoordeling_bullets = make_assessment_points(conv)

    wetgeving_text = "\n".join(f"- {item}" for item in relevante_wetgeving) or "- Nog te koppelen"
    bullets_text = "\n".join(f"- {item}" for item in beoordeling_bullets) or "- Nog te beoordelen"

    return textwrap.dedent(
        f"""
        ---
        titel: {yaml_escape(title)}
        datum: {datum}
        status: concept
        tags: {format_yaml_list(tags)}
        vraagbestand: {yaml_escape(relative_path(output_root, paths.vraag))}
        antwoordbestand: {yaml_escape(relative_path(output_root, paths.antwoord))}
        eigenaar: compliance
        samenvatting: {yaml_escape(samenvatting)}
        ---

        # Beoordeling

        ## Casus
        {title}

        ## Samenvatting
        {samenvatting}

        ## Relevante wetgeving / normen / kaders
        {wetgeving_text}

        ## Beoordeling
        {bullets_text}

        ## Uniform standpunt
        - Nog vast te stellen

        ## Benodigde check met specialist
        - Ja/Nee, nog te bepalen

        ## Dossierverwijzingen
        - Vraag: `{relative_path(output_root, paths.vraag)}`
        - Antwoord: `{relative_path(output_root, paths.antwoord)}`
        """
    ).strip() + "\n"


def render_antwoord_markdown(conv: ConversationRecord, output_root: Path, paths: CasePaths) -> str:
    datum = format_case_date(conv)
    title = conv.title
    antwoordtekst = first_message_text(conv, "assistant") or "Nog op te stellen"
    samenvatting = make_short_summary(conv)
    tags = format_yaml_list(derive_tags(conv))
    afzender_type = infer_sender_type(conv)

    return textwrap.dedent(
        f"""
        ---
        titel: {yaml_escape(title)}
        datum: {datum}
        doelgroep: {yaml_escape(afzender_type)}
        status: concept
        tags: {tags}
        vraagbestand: {yaml_escape(relative_path(output_root, paths.vraag))}
        beoordelingsbestand: {yaml_escape(relative_path(output_root, paths.beoordeling))}
        samenvatting: {yaml_escape(samenvatting)}
        ---

        # Antwoord

        ## Onderwerp
        {title}

        ## Conceptantwoord
        {antwoordtekst}

        ## Onderbouwing
        - Toets aan beoordeling: `{relative_path(output_root, paths.beoordeling)}`
        - Controle op locatiespecifieke afwijkingen: nog uitvoeren

        ## Follow-up
        - Leg definitieve versie vast zodra antwoord extern is verzonden
        """
    ).strip() + "\n"


def build_indexes(root: Path) -> None:
    cases = collect_case_index_records(root)
    write_index_vragen(root, cases)
    write_index_antwoorden(root, cases)
    write_index_themas(root, cases)
    write_index_locaties(root, cases)
    write_index_casus_tijdlijn(root, cases)
    write_index_wetgeving(root)


def collect_case_index_records(root: Path) -> list[CaseIndexRecord]:
    vraag_root = root / "02_Vragen"
    beoordeling_root = root / "03_Beoordelingen"
    antwoord_root = root / "04_Antwoorden"

    beoordeling_map = build_relative_stem_map(root, beoordeling_root)
    antwoord_map = build_relative_stem_map(root, antwoord_root)

    items: list[CaseIndexRecord] = []
    for vraag_path in sorted(vraag_root.glob("**/*.md")):
        meta = read_frontmatter(vraag_path)
        stem = vraag_path.stem.removesuffix("_vraag")
        date = first_non_empty(meta.get("datum"), vraag_path.parent.name)
        items.append(
            CaseIndexRecord(
                title=first_non_empty(meta.get("titel"), stem),
                date=date,
                year=extract_year(date, vraag_path.parent.name),
                tags=extract_frontmatter_tags(meta),
                vraag_rel=relative_path(root, vraag_path),
                beoordeling_rel=beoordeling_map.get(stem, ""),
                antwoord_rel=antwoord_map.get(stem, ""),
                locatie=first_non_empty(meta.get("locatie"), "onbekend"),
                afzender_type=first_non_empty(meta.get("afzender_type"), "onbekend"),
                status=first_non_empty(meta.get("status"), "nieuw"),
            )
        )
    return sorted(items, key=lambda item: (item.date, item.title.casefold()))


def build_relative_stem_map(root: Path, base_dir: Path) -> dict[str, str]:
    mapping: dict[str, str] = {}
    suffix = {
        "03_Beoordelingen": "_beoordeling",
        "04_Antwoorden": "_antwoord",
    }.get(base_dir.name, "")
    for path in sorted(base_dir.glob("**/*.md")):
        mapping[path.stem.removesuffix(suffix)] = relative_path(root, path)
    return mapping


def write_index_vragen(root: Path, items: list[CaseIndexRecord]) -> None:
    lines = ["# Index Vragen", "", "## Overzicht", ""]
    for item in items:
        lines.append(f"- [{item.title}]({item.vraag_rel}) — {item.date} — {item.locatie} — {item.afzender_type}")
    write_text(root / "08_Index" / "index_vragen.md", lines)


def write_index_antwoorden(root: Path, items: list[CaseIndexRecord]) -> None:
    lines = ["# Index Antwoorden", ""]
    for item in items:
        lines.append(f"## {item.title}")
        lines.append(f"- Vraag: `{item.vraag_rel}`")
        lines.append(f"- Beoordeling: `{item.beoordeling_rel or 'ontbreekt'}`")
        lines.append(f"- Antwoord: `{item.antwoord_rel or 'ontbreekt'}`")
        lines.append("")
    write_text(root / "08_Index" / "index_antwoorden.md", lines)


def write_index_themas(root: Path, items: list[CaseIndexRecord]) -> None:
    grouped: dict[str, list[CaseIndexRecord]] = defaultdict(list)
    for item in items:
        for tag in item.tags:
            grouped[tag].append(item)

    lines = ["# Index Thema's", ""]
    for tag in sorted(grouped):
        lines.append(f"## {tag}")
        for item in grouped[tag]:
            lines.append(f"- [{item.title}]({item.vraag_rel}) — {item.date}")
        lines.append("")
    write_text(root / "08_Index" / "index_themas.md", lines)


def write_index_locaties(root: Path, items: list[CaseIndexRecord]) -> None:
    grouped: dict[str, list[CaseIndexRecord]] = defaultdict(list)
    for item in items:
        grouped[item.locatie].append(item)

    lines = ["# Index Locaties", ""]
    for locatie in sorted(grouped):
        lines.append(f"## {locatie}")
        for item in grouped[locatie]:
            lines.append(f"- [{item.title}]({item.vraag_rel}) — {item.date} — status: {item.status}")
        lines.append("")
    write_text(root / "08_Index" / "index_locaties.md", lines)


def write_index_casus_tijdlijn(root: Path, items: list[CaseIndexRecord]) -> None:
    grouped: dict[str, list[CaseIndexRecord]] = defaultdict(list)
    for item in items:
        grouped[item.year].append(item)

    lines = ["# Index Casus Tijdlijn", ""]
    for year in sorted(grouped):
        lines.append(f"## {year}")
        for item in grouped[year]:
            lines.append(f"- {item.date} — [{item.title}]({item.vraag_rel})")
        lines.append("")
    write_text(root / "08_Index" / "index_casus_tijdlijn.md", lines)


def write_index_wetgeving(root: Path) -> None:
    wetgeving_root = root / "01_Wetgeving"
    lines = ["# Index Wetgeving", ""]
    found_any = False
    for category in sorted(path for path in wetgeving_root.iterdir() if path.is_dir()):
        entries = sorted(category.glob("*.md"))
        lines.append(f"## {category.name}")
        if not entries:
            lines.append("- Nog geen documenten geregistreerd")
        else:
            found_any = True
            for path in entries:
                meta = read_frontmatter(path)
                title = first_non_empty(meta.get("titel"), path.stem)
                onderwerp = first_non_empty(meta.get("onderwerp"), "nog_te_bepalen")
                lines.append(f"- [{title}]({relative_path(root, path)}) — onderwerp: {onderwerp}")
        lines.append("")
    if not found_any:
        lines.append("Gebruik `07_Templates/template_wetgeving.md` om wetgeving toe te voegen.")
    write_text(root / "08_Index" / "index_wetgeving.md", lines)


def read_frontmatter(path: Path) -> dict[str, str]:
    text = path.read_text(encoding="utf-8")
    if not text.startswith(FRONTMATTER_DELIMITER):
        return {}

    meta: dict[str, str] = {}
    for line in text.splitlines()[1:]:
        if line.strip() == FRONTMATTER_DELIMITER:
            break
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        meta[key.strip()] = value.strip()
    return meta


def extract_frontmatter_tags(meta: dict[str, str]) -> list[str]:
    raw = meta.get("tags", "").strip()
    if not raw:
        return []
    if raw.startswith("[") and raw.endswith("]"):
        inner = raw[1:-1].strip()
        if not inner:
            return []
        return [clean_tag(part) for part in inner.split(",") if clean_tag(part)]
    tag = clean_tag(raw)
    return [tag] if tag else []


def clean_tag(value: str) -> str:
    return value.strip().strip('"').strip("'")


def derive_tags(conv: ConversationRecord) -> list[str]:
    text = " ".join([conv.title] + [message.text for message in conv.messages[:8]]).lower()
    vocabulary = [
        "reach",
        "clp",
        "sds",
        "adr",
        "brzo",
        "pgs15",
        "arbo",
        "omgevingswet",
        "bal",
        "vos",
        "afval",
        "opslag",
        "etikettering",
        "transport",
        "emissie",
        "veiligheid",
        "gevaarlijke_stoffen",
        "leverancier",
        "klant",
        "compliance",
    ]
    normalized_text = text.replace("gevaarlijke stoffen", "gevaarlijke_stoffen")
    found = [tag for tag in vocabulary if tag in normalized_text]
    if not found:
        found = ["compliance", "casus"]
    return sorted(dict.fromkeys(found))


def suggest_regulations(tags: list[str]) -> list[str]:
    mapping = {
        "reach": "REACH-verordening: stofregistratie, SVHC en restricties controleren",
        "clp": "CLP-verordening: classificatie, etikettering en verpakking toetsen",
        "sds": "REACH bijlage II / SDS-verplichtingen: veiligheidsinformatieblad beoordelen",
        "adr": "ADR: transport van gevaarlijke stoffen controleren",
        "brzo": "Brzo / Seveso: drempelwaarden en veiligheidsbeheersysteem toetsen",
        "pgs15": "PGS 15: opslag van verpakte gevaarlijke stoffen controleren",
        "arbo": "Arbowet en Arbobesluit: blootstelling en werkplekinstructies meenemen",
        "omgevingswet": "Omgevingswet / Bal: activiteiten en vergunningplichten toetsen",
        "bal": "Besluit activiteiten leefomgeving: emissies en milieuregels controleren",
        "vos": "VOS-regelgeving: oplosmiddelengebruik en emissiegrenzen toetsen",
        "afval": "Afvalstoffenregelgeving: classificatie, afgifte en registratie beoordelen",
        "opslag": "Opslagkaders: vergunning, PGS en interne procedures toetsen",
        "etikettering": "CLP en productspecifieke etiketteringseisen controleren",
        "transport": "ADR en vervoersdocumentatie meenemen in beoordeling",
        "emissie": "Milieuregels, vergunningen en emissie-eisen toetsen",
        "veiligheid": "Interne veiligheidsprocedures en externe wettelijke eisen combineren",
        "gevaarlijke_stoffen": "Wet- en regelgeving voor gevaarlijke stoffen integraal beoordelen",
    }
    suggestions = [mapping[tag] for tag in tags if tag in mapping]
    return suggestions or ["Nog handmatig koppelen aan relevante wetgeving en interne normdocumenten"]


def make_short_summary(conv: ConversationRecord, max_len: int = 220) -> str:
    base = first_message_text(conv, "user") or (conv.messages[0].text.strip() if conv.messages else conv.title)
    base = normalize_whitespace(base)
    return base if len(base) <= max_len else base[: max_len - 1].rstrip() + "…"


def make_assessment_points(conv: ConversationRecord, max_items: int = 5) -> list[str]:
    bullets: list[str] = []
    user_text = first_message_text(conv, "user")
    assistant_text = first_message_text(conv, "assistant")
    if user_text:
        bullets.append(f"Vraag scope: {truncate(normalize_whitespace(user_text), 180)}")
    if assistant_text:
        bullets.append(f"Bestaand conceptantwoord: {truncate(normalize_whitespace(assistant_text), 180)}")
    for tag in derive_tags(conv)[: max_items - len(bullets)]:
        bullets.append(f"Thema ter toetsing: {tag}")
    return bullets[:max_items]


def first_message_text(conv: ConversationRecord, role: str) -> str:
    for message in conv.messages:
        if message.role == role and message.text.strip():
            return message.text.strip()
    return ""


def infer_sender_type(conv: ConversationRecord) -> str:
    source = (first_message_text(conv, "user") + " " + conv.title).lower()
    if "leverancier" in source or "supplier" in source:
        return "leverancier"
    if "klant" in source or "customer" in source:
        return "klant"
    return "onbekend"


def infer_location(conv: ConversationRecord) -> str:
    source = (first_message_text(conv, "user") + " " + conv.title).lower()
    for marker in ["locatie ", "site ", "vestiging "]:
        index = source.find(marker)
        if index >= 0:
            snippet = source[index : index + 40]
            return normalize_whitespace(snippet).split(".")[0][:30]
    return "centrale_beoordeling"


def render_full_thread(conv: ConversationRecord) -> str:
    if not conv.messages:
        return "Geen berichtinhoud gevonden in export."
    blocks: list[str] = []
    for message in conv.messages:
        role_label = role_to_label(message.role)
        timestamp = message.created_at.isoformat() if message.created_at else ""
        header = f"### {role_label}"
        if timestamp:
            header += f" ({timestamp})"
        blocks.append(f"{header}\n\n{message.text.strip()}")
    return "\n\n".join(blocks)


def format_case_date(conv: ConversationRecord) -> str:
    return (conv.update_time or conv.create_time or datetime.now(tz=timezone.utc)).strftime("%Y-%m-%d")


def normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def truncate(text: str, max_len: int) -> str:
    return text if len(text) <= max_len else text[: max_len - 1].rstrip() + "…"


def clean_title(value: str) -> str:
    cleaned = normalize_whitespace(value)
    return cleaned or "ongespecificeerde casus"


def role_to_label(role: str) -> str:
    return {
        "user": "Gebruiker",
        "assistant": "Assistent",
        "system": "Systeem",
        "tool": "Tool",
    }.get(role, role.capitalize() if role else "Onbekend")


def slugify(text: str, max_len: int = 80) -> str:
    text = text.lower().strip()
    replacements = {
        "á": "a", "à": "a", "ä": "a", "â": "a",
        "é": "e", "è": "e", "ë": "e", "ê": "e",
        "í": "i", "ì": "i", "ï": "i", "î": "i",
        "ó": "o", "ò": "o", "ö": "o", "ô": "o",
        "ú": "u", "ù": "u", "ü": "u", "û": "u",
        "ç": "c", "ñ": "n",
    }
    for src, dst in replacements.items():
        text = text.replace(src, dst)
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text[:max_len].strip("_") or "ongespecificeerd"


def yaml_escape(text: str) -> str:
    text = str(text).replace("\n", " ").strip()
    if not text:
        return '""'
    if any(ch in text for ch in [":", "#", "[", "]", "{", "}", '"', "'"]):
        escaped = text.replace('"', '\\"')
        return f'"{escaped}"'
    return text


def format_yaml_list(values: list[str]) -> str:
    return "[" + ", ".join(yaml_escape(value) for value in values) + "]"


def first_non_empty(*values: str | None) -> str:
    for value in values:
        if value is None:
            continue
        cleaned = str(value).strip().strip('"')
        if cleaned:
            return cleaned
    return ""


def extract_year(date_text: str, fallback: str) -> str:
    match = re.match(r"(\d{4})-\d{2}-\d{2}$", date_text)
    return match.group(1) if match else fallback


def relative_path(root: Path, path: Path) -> str:
    return str(path.relative_to(root)).replace("\\", "/")


def write_text(path: Path, lines: list[str]) -> None:
    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


README_TEXT = """
# ChemieComplianceKennisbank

Deze repository is een centrale databank voor wetgeving, casuïstiek, beoordelingen en uniforme antwoorden voor een chemisch bedrijf.

## Doel
- relevante wetgeving en normen centraal vastleggen
- klant- en leveranciersvragen uniform beoordelen
- antwoorden herbruikbaar opslaan voor alle locaties
- consistentie in compliance-beoordeling vergroten

## Structuur
- `00_Inbox/` = ruwe invoer en exports
- `01_Wetgeving/` = wetgeving, normen en richtlijnen
- `02_Vragen/` = geregistreerde klant- en leveranciersvragen
- `03_Beoordelingen/` = juridische/compliance-toetsingen en standpunten
- `04_Antwoorden/` = concept- en definitieve antwoorden
- `05_Bronnen/` = bronbestanden zoals pdf, spreadsheets, e-mails en interne documenten
- `06_Locaties/` = locatiespecifieke profielen en afwijkingen
- `07_Templates/` = standaardformats
- `08_Index/` = navigatie- en overzichtsbestanden
- `09_Workflows/` = werkinstructies voor beheer en gebruik

## Werkwijze
1. Leg wetgeving vast in `01_Wetgeving/`
2. Registreer elke klant- of leveranciersvraag in `02_Vragen/`
3. Leg de beoordeling vast in `03_Beoordelingen/`
4. Sla het uniforme antwoord op in `04_Antwoorden/`
5. Verwerk locatiespecifieke afwijkingen in `06_Locaties/`
6. Houd indexen actueel via `08_Index/`
"""

AGENTS_TEXT = """
# AGENTS.md

## Rol
Deze repository is een centrale compliance-kennisbank voor een chemisch bedrijf.

## Doel voor Codex
Help met:
- structureren van wetgeving en normen
- registreren van vragen van klanten en leveranciers
- toetsen van casussen aan relevante wetgeving
- opstellen van consistente antwoorden
- signaleren van verschillen tussen locaties
- bewaken van uniformiteit en hergebruik van eerdere beoordelingen

## Werkwijze
1. Gebruik `01_Wetgeving/` als primaire bron voor regelgeving.
2. Gebruik `03_Beoordelingen/` voor het geldende interne standpunt.
3. Gebruik `04_Antwoorden/` alleen als afgeleide van de beoordeling.
4. Leg locatiespecifieke uitzonderingen expliciet vast in `06_Locaties/`.
5. Werk indexbestanden in `08_Index/` bij als nieuwe dossiers worden toegevoegd.

## Outputstijl
- Kort en zakelijk
- Geen marketingtaal
- Duidelijk onderscheid tussen feit, interpretatie en advies
- Verwijs expliciet naar relevante wetgeving of interne beoordeling
"""

GITIGNORE_TEXT = """
.DS_Store
Thumbs.db
*.tmp
*.bak
__pycache__/
"""

TEMPLATE_WETGEVING = """
---
titel:
datum:
jurisdictie:
onderwerp:
bron:
status: actief
tags: []
samenvatting:
---

# Wetgeving

## Onderwerp
...

## Reikwijdte
...

## Relevantie voor het bedrijf
...

## Kernverplichtingen
- ...

## Toetsingspunten
- ...

## Bronnen
- ...
"""

TEMPLATE_VRAAG = """
---
titel:
datum:
locatie:
afzender_type:
status: nieuw
tags: []
beoordeling:
antwoord:
samenvatting:
---

# Vraag

## Onderwerp
...

## Herkomst
- Type afzender:
- Locatie:
- Contactpersoon:

## Vraagstelling
...

## Bijlagen / bronverwijzingen
- ...
"""

TEMPLATE_BEOORDELING = """
---
titel:
datum:
status: concept
tags: []
vraagbestand:
antwoordbestand:
eigenaar: compliance
samenvatting:
---

# Beoordeling

## Casus
...

## Relevante wetgeving / normen / kaders
- ...

## Analyse
- ...

## Uniform standpunt
- ...

## Open punten
- ...
"""

TEMPLATE_ANTWOORD = """
---
titel:
datum:
doelgroep:
status: concept
tags: []
vraagbestand:
beoordelingsbestand:
samenvatting:
---

# Antwoord

## Onderwerp
...

## Conceptantwoord
...

## Onderbouwing
- ...

## Verzending / follow-up
- ...
"""

TEMPLATE_LOCATIEPROFIEL = """
---
locatie:
datum:
status: actief
tags: []
---

# Locatieprofiel

## Locatie
...

## Relevante vergunningen / afwijkingen
- ...

## Contactpersonen
- ...

## Bijzondere aandachtspunten
- ...
"""

STARTDOC_REACH = """
---
titel: REACH-verordening
datum: 2026-04-14
jurisdictie: eu
onderwerp: stoffenregistratie en keteninformatie
bron: eur-lex
status: actief
tags: [reach, sds, gevaarlijke_stoffen, compliance]
samenvatting: Basisdocument voor registratie, restricties, autorisaties en informatieverplichtingen rond chemische stoffen.
---

# Wetgeving

## Onderwerp
REACH vormt het basisraamwerk voor registratie, beoordeling, autorisatie en restrictie van chemische stoffen binnen de EU.

## Reikwijdte
Relevant voor productie, import, gebruik, distributie en communicatie in de keten van chemische stoffen en mengsels.

## Relevantie voor het bedrijf
Van belang bij stofregistratie, SVHC-beoordeling, restricties, communicatie in het veiligheidsinformatieblad en verplichtingen richting klanten en leveranciers.

## Kernverplichtingen
- Beoordeel of stoffen geregistreerd moeten zijn voor de relevante toepassing.
- Controleer of stoffen op kandidaatslijsten, restrictielijsten of autorisatielijsten staan.
- Borg correcte keteninformatie richting afnemers en leveranciers.
- Toets of het veiligheidsinformatieblad actueel en passend is.

## Toetsingspunten
- Is de stof of het mengsel geïdentificeerd en juist geclassificeerd?
- Zijn er restricties of autorisaties van toepassing?
- Is er een actueel SDS of uitgebreide SDS beschikbaar?
- Zijn gebruiksscenario's en blootstellingsvoorwaarden passend voor de klantvraag?

## Bronnen
- Eur-Lex publicatie van REACH
- ECHA guidance en stofinformatie
"""

STARTDOC_CLP = """
---
titel: CLP-verordening
datum: 2026-04-14
jurisdictie: eu
onderwerp: classificatie etikettering en verpakking
bron: eur-lex
status: actief
tags: [clp, etikettering, gevaarlijke_stoffen, compliance]
samenvatting: Basisdocument voor classificatie, etikettering en verpakking van stoffen en mengsels.
---

# Wetgeving

## Onderwerp
CLP bepaalt hoe stoffen en mengsels moeten worden geclassificeerd, geëtiketteerd en verpakt binnen de EU.

## Reikwijdte
Relevant voor producten, etiketten, verpakkingen, gevarenpictogrammen, H- en P-zinnen en communicatie in de keten.

## Relevantie voor het bedrijf
Van belang voor etiketvragen, productsamenstelling, heretikettering, private label situaties en consistente beantwoording naar klanten en leveranciers.

## Kernverplichtingen
- Bepaal en documenteer de juiste classificatie.
- Gebruik correcte etikettering, pictogrammen en signaalwoorden.
- Controleer verpakkingseisen en leesbaarheid van informatie.
- Borg samenhang tussen CLP-indeling en SDS-informatie.

## Toetsingspunten
- Klopt de classificatie met de meest recente stof- en mengseldata?
- Zijn etiketonderdelen volledig en consistent?
- Zijn taal- en landspecifieke eisen beoordeeld?
- Sluit het SDS aan op het etiket?

## Bronnen
- Eur-Lex publicatie van CLP
- ECHA guidance over classification and labelling
"""

STARTDOC_ADR = """
---
titel: ADR transport gevaarlijke stoffen
datum: 2026-04-14
jurisdictie: normen_en_richtlijnen
onderwerp: vervoer over de weg
bron: adr
status: actief
tags: [adr, transport, gevaarlijke_stoffen, compliance]
samenvatting: Basisdocument voor vervoer van gevaarlijke stoffen over de weg inclusief classificatie, verpakking en documentatie.
---

# Wetgeving

## Onderwerp
ADR bevat de eisen voor vervoer van gevaarlijke stoffen over de weg.

## Reikwijdte
Relevant voor classificatie, UN-nummers, verpakkingsgroepen, etikettering, vervoersdocumenten, tunnelcodes en opleiding.

## Relevantie voor het bedrijf
Van belang bij klant- en leveranciersvragen over verzending, verpakking, labeling, documentatie en transportverantwoordelijkheden.

## Kernverplichtingen
- Bepaal juiste transportclassificatie en UN-nummer.
- Toets verpakkingsinstructies en etikettering voor transport.
- Borg correcte vervoersdocumentatie en eventuele vrijstellingen.
- Controleer taken van afzender, vervoerder en verlader.

## Toetsingspunten
- Is de transportclassificatie juist en gedocumenteerd?
- Zijn verpakkingen en labels ADR-conform?
- Is de documentatie volledig?
- Zijn eventuele uitzonderingen of limited quantities correct toegepast?

## Bronnen
- ADR-tekst en nationale uitvoeringsinformatie
"""

STARTDOC_PGS15 = """
---
titel: PGS 15 opslag verpakte gevaarlijke stoffen
datum: 2026-04-14
jurisdictie: nationaal
onderwerp: opslag van verpakte gevaarlijke stoffen
bron: pgs
status: actief
tags: [pgs15, opslag, veiligheid, gevaarlijke_stoffen, compliance]
samenvatting: Basisdocument voor veilige opslag van verpakte gevaarlijke stoffen en CMR-stoffen.
---

# Wetgeving

## Onderwerp
PGS 15 geeft invulling aan veilige opslag van verpakte gevaarlijke stoffen en aanverwante producten.

## Reikwijdte
Relevant voor opslagvoorzieningen, scheiding van stoffen, brandveiligheid, vakbekwaamheid en operationele beheersmaatregelen.

## Relevantie voor het bedrijf
Belangrijk voor vragen over opslagcondities, maximale hoeveelheden, incompatibele combinaties en inrichting van magazijnen.

## Kernverplichtingen
- Toets opslagvoorzieningen aan de geldende PGS 15-voorschriften.
- Beoordeel scheiding van incompatibele stoffen.
- Borg eisen rond brandveiligheid, inspectie en incidentbeheersing.
- Koppel de eisen aan vergunning- en locatiespecifieke randvoorwaarden.

## Toetsingspunten
- Valt de opslag onder PGS 15 en zo ja onder welke situatie?
- Zijn hoeveelheden, verpakkingen en opslagklassen juist beoordeeld?
- Zijn aanvullende vergunningseisen van toepassing?
- Is de locatie-inrichting aantoonbaar passend?

## Bronnen
- Publicatiereeks Gevaarlijke Stoffen 15
"""

STARTDOC_BRZO = """
---
titel: Brzo / Seveso
datum: 2026-04-14
jurisdictie: nationaal
onderwerp: zware ongevallen met gevaarlijke stoffen
bron: overheid
status: actief
tags: [brzo, veiligheid, gevaarlijke_stoffen, compliance]
samenvatting: Basisdocument voor drempelwaarden, veiligheidsbeheersystemen en verplichtingen rond zware ongevallen.
---

# Wetgeving

## Onderwerp
Brzo geeft regels voor bedrijven waar gevaarlijke stoffen in hoeveelheden aanwezig kunnen zijn die zware ongevallen mogelijk maken.

## Reikwijdte
Relevant voor drempelwaarden, kennisgeving, veiligheidsrapporten, veiligheidsbeheersysteem en noodplanning.

## Relevantie voor het bedrijf
Belangrijk voor locatievragen, wijzigingsbeoordelingen, stofhoeveelheden en de vraag of aanvullende Seveso-verplichtingen gelden.

## Kernverplichtingen
- Toets aanwezigheid van gevaarlijke stoffen aan Brzo-drempels.
- Borg veiligheidsbeheersysteem en wijzigingsmanagement.
- Controleer kennisgevings- en rapportageplichten.
- Stem af met vergunningen en externe veiligheid.

## Toetsingspunten
- Worden drempelwaarden overschreden of benaderd?
- Zijn stofcategorieën correct ingedeeld?
- Zijn organisatorische en technische beheersmaatregelen aantoonbaar?
- Heeft de klantvraag impact op Brzo-status of scenario's?

## Bronnen
- Brzo-regelgeving en Seveso-kaders
"""

STARTDOC_BAL = """
---
titel: Besluit activiteiten leefomgeving
datum: 2026-04-14
jurisdictie: nationaal
onderwerp: milieuregels voor activiteiten
bron: overheid
status: actief
tags: [bal, omgevingswet, emissie, compliance]
samenvatting: Basisdocument voor milieuregels onder de Omgevingswet, waaronder emissies, activiteiten en meld- of vergunningplichten.
---

# Wetgeving

## Onderwerp
Het Bal bevat algemene milieuregels voor activiteiten in de leefomgeving, inclusief regels voor emissies en milieubelastende activiteiten.

## Reikwijdte
Relevant voor productieactiviteiten, emissies naar lucht en water, opslag, afvalstromen en meld- of vergunningplichten.

## Relevantie voor het bedrijf
Belangrijk bij vragen over operationele activiteiten, emissiegrenzen, milieubelastende activiteiten en locatiespecifieke milieuregels.

## Kernverplichtingen
- Bepaal of de activiteit onder algemene regels, meldplicht of vergunningplicht valt.
- Toets emissies en operationele randvoorwaarden.
- Koppel activiteitseisen aan lokale vergunningen en maatwerkvoorschriften.
- Borg aantoonbaarheid in procedures en registraties.

## Toetsingspunten
- Welke activiteit uit het Bal is van toepassing?
- Gelden emissiegrenzen of specifieke technische eisen?
- Zijn lokale maatwerkregels of vergunningvoorschriften aanvullend van toepassing?
- Heeft de vraag impact op bestaande vergunningen of meldingen?

## Bronnen
- Omgevingswet en Besluit activiteiten leefomgeving
"""

IMPORT_PROCEDURE = """
# Importprocedure

## Doel
Nieuwe casussen uit gesprekken of exports omzetten naar een herbruikbaar compliance-dossier.

## Stappen
1. Plaats ruwe export in `00_Inbox/`
2. Importeer de casus naar `02_Vragen/`, `03_Beoordelingen/` en `04_Antwoorden/`
3. Vul relevante wetgeving aan in `01_Wetgeving/`
4. Werk locatiespecifieke uitzonderingen uit in `06_Locaties/`
5. Controleer de indexbestanden in `08_Index/`
6. Markeer definitieve antwoorden pas na inhoudelijke review
"""

CODEX_QUERIES = """
# Voorbeeldqueries voor Codex

## Wetgeving
- Welke dossiers verwijzen naar REACH of CLP?
- Welke wetgeving is relevant voor opslag van gevaarlijke stoffen?

## Casussen
- Geef alle klantvragen over etikettering of SDS.
- Welke leveranciersvragen kwamen binnen voor locatie X?

## Uniformiteit
- Zijn er antwoorden met hetzelfde onderwerp maar verschillende conclusies?
- Welke locaties hebben afwijkende aandachtspunten voor dezelfde casus?

## Beheer
- Welke vragen hebben nog geen beoordeling?
- Welke beoordelingen missen gekoppelde wetgeving?
- Welke antwoorden staan nog op concept?
"""


if __name__ == "__main__":
    raise SystemExit(main())
