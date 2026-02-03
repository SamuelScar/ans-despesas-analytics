from pathlib import Path
from typing import Dict

import pandas as pd


INPUT_FILE = Path("data/output/consolidado_validado.csv")
CADOP_PATH = Path("Relatorio_cadop.csv")
OUTPUT_DIR = Path("data/output")
OUTPUT_FILE = OUTPUT_DIR / "consolidado_enriquecido.csv"
MISSING_FILE = OUTPUT_DIR / "inconsistencias_2_2.csv"
CADOP_ENCODINGS = ["utf-8-sig", "utf-8", "latin-1"]


def _load_cadop(path: Path) -> "pd.DataFrame":
    """
    Carrega o CADOP e normaliza colunas para join por CNPJ.

    :param path: Caminho do CADOP.
    :return: DataFrame do CADOP normalizado.
    """
    cadop_df = None
    last_error = None
    for enc in CADOP_ENCODINGS:
        try:
            cadop_df = pd.read_csv(path, sep=";", encoding=enc, dtype=str)
            break
        except Exception as exc:
            last_error = exc
            continue
    if cadop_df is None:
        raise ValueError(f"Falha ao ler CADOP: {last_error}")

    cols = {c.lower().replace("_", ""): c for c in cadop_df.columns}
    reg_col = cols.get("registrooperadora")
    cnpj_col = cols.get("cnpj")
    mod_col = cols.get("modalidade")
    uf_col = cols.get("uf")
    data_col = cols.get("dataregistroans")

    if not reg_col or not cnpj_col or not mod_col or not uf_col:
        raise ValueError("Colunas obrigatorias nao encontradas no CADOP.")

    keep_cols = [reg_col, cnpj_col, mod_col, uf_col] + ([data_col] if data_col else [])
    cadop_df = cadop_df[keep_cols].copy()
    cadop_df.columns = ["RegistroANS", "CNPJ", "Modalidade", "UF"] + (
        ["DataRegistroANS"] if data_col else []
    )
    cadop_df["CNPJ"] = cadop_df["CNPJ"].str.replace(r"\D", "", regex=True)

    cadop_df = cadop_df.dropna(subset=["CNPJ"])
    if "DataRegistroANS" in cadop_df.columns:
        cadop_df["DataRegistroANS"] = pd.to_datetime(
            cadop_df["DataRegistroANS"], errors="coerce"
        )
        cadop_df = cadop_df.sort_values("DataRegistroANS")
    cadop_df = cadop_df.drop_duplicates(subset=["CNPJ"], keep="last")
    return cadop_df


def enrich(
    input_file: Path = INPUT_FILE,
    cadop_path: Path = CADOP_PATH,
    output_file: Path = OUTPUT_FILE,
    missing_file: Path = MISSING_FILE,
) -> None:
    """
    Faz join com CADOP e salva o consolidado enriquecido.

    :param input_file: CSV consolidado validado.
    :param cadop_path: Caminho do CADOP.
    :param output_file: CSV enriquecido de saida.
    :param missing_file: CSV de inconsistencias.
    :return: None.
    """
    consolidated_df = pd.read_csv(input_file, dtype=str, encoding="utf-8-sig")
    consolidated_df["CNPJ"] = consolidated_df["CNPJ"].str.replace(r"\D", "", regex=True)
    cadop_df = _load_cadop(cadop_path)

    enriched_df = consolidated_df.merge(cadop_df, on="CNPJ", how="left")
    missing_df = enriched_df[enriched_df["RegistroANS"].isna()].copy()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    enriched_df.to_csv(output_file, index=False, encoding="utf-8-sig")
    missing_df.to_csv(missing_file, index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    enrich()
    print(f"CSV gerado: {OUTPUT_FILE}")
    print(f"Inconsistencias: {MISSING_FILE}")
