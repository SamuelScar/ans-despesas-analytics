from pathlib import Path

import pandas as pd


INPUT_FILE = Path("data/output/consolidado_enriquecido.csv")
OUTPUT_DIR = Path("data/output")
OUTPUT_FILE = OUTPUT_DIR / "despesas_agregadas.csv"


def _parse_valor(value: object) -> float:
    """
    Converte valor monetario tratando separadores decimais.

    :param value: Valor original.
    :return: Valor convertido.
    """
    if value is None:
        return 0.0
    text = str(value).strip()
    if not text:
        return 0.0
    if "," in text and "." in text:
        text = text.replace(".", "").replace(",", ".")
    elif "," in text:
        text = text.replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return 0.0


def _std_pop(series: "pd.Series") -> float:
    """
    Calcula desvio padrao populacional (ddof=0).

    :param series: Serie numerica.
    :return: Desvio padrao populacional.
    """
    return float(series.std(ddof=0))


def aggregate(
    input_file: Path = INPUT_FILE,
    output_file: Path = OUTPUT_FILE,
) -> None:
    """
    Agrega despesas por RazaoSocial e UF.

    :param input_file: CSV enriquecido de entrada.
    :param output_file: CSV agregado de saida.
    :return: None.
    """
    enriched_df = pd.read_csv(input_file, dtype=str, encoding="utf-8-sig")
    enriched_df["RazaoSocial"] = (
        enriched_df["RazaoSocial"].fillna("").astype(str).str.strip()
    )
    enriched_df["UF"] = enriched_df["UF"].fillna("").astype(str).str.strip()

    enriched_df = enriched_df[
        (enriched_df["RazaoSocial"] != "") & (enriched_df["UF"] != "")
    ]
    enriched_df["ValorDespesas_num"] = enriched_df["ValorDespesas"].map(_parse_valor)

    aggregated_df = (
        enriched_df.groupby(["RazaoSocial", "UF"])["ValorDespesas_num"]
        .agg(
            TotalDespesas="sum",
            MediaDespesas="mean",
            DesvioPadraoDespesas=_std_pop,
        )
        .reset_index()
    )

    aggregated_df["DesvioPadraoDespesas"] = aggregated_df[
        "DesvioPadraoDespesas"
    ].fillna(0.0)
    aggregated_df = aggregated_df.sort_values("TotalDespesas", ascending=False)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    for col in ["TotalDespesas", "MediaDespesas", "DesvioPadraoDespesas"]:
        aggregated_df[col] = aggregated_df[col].map(lambda v: f"{v:.5f}")
    aggregated_df.to_csv(output_file, index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    aggregate()
    print(f"CSV gerado: {OUTPUT_FILE}")
