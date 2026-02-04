import re
from pathlib import Path
from typing import List

import pandas as pd


INPUT_FILE = Path("data/output/consolidado_despesas.csv")
OUTPUT_DIR = Path("data/output")
VALID_FILE = OUTPUT_DIR / "consolidado_validado.csv"
INVALID_FILE = OUTPUT_DIR / "inconsistencias_2_1.csv"


def _only_digits(value: object) -> str:
    """
    Retorna apenas os digitos de um valor.

    :param value: Valor original.
    :return: String apenas com digitos.
    """
    if value is None:
        return ""
    return re.sub(r"\D", "", str(value))


def _cnpj_is_valid(value: object) -> bool:
    """
    Valida CNPJ usando digitos verificadores.

    :param value: Valor do CNPJ.
    :return: True se valido.
    """
    digits = _only_digits(value)
    if len(digits) != 14:
        return False
    if digits == digits[0] * 14:
        return False

    def _calc_digit(base: str, weights: List[int]) -> str:
        """
        Calcula um digito verificador do CNPJ com pesos informados.

        :param base: Base numerica.
        :param weights: Pesos de calculo.
        :return: Digito verificador.
        """
        total = sum(int(d) * w for d, w in zip(base, weights))
        mod = total % 11
        return "0" if mod < 2 else str(11 - mod)

    d1 = _calc_digit(digits[:12], [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2])
    d2 = _calc_digit(digits[:12] + d1, [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2])
    return digits[-2:] == d1 + d2


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


def _append_reason(base: "pd.Series", reason: str, mask: "pd.Series") -> "pd.Series":
    """
    Anexa um motivo ao texto base quando a mascara e verdadeira.

    :param base: Serie com motivos atuais.
    :param reason: Motivo a adicionar.
    :param mask: Mascara booleana onde o motivo se aplica.
    :return: Serie atualizada.
    """
    updated = base.copy()
    to_update = mask & base.eq("")
    to_append = mask & base.ne("")
    updated.loc[to_update] = reason
    updated.loc[to_append] = updated.loc[to_append] + ";" + reason
    return updated


def validate(
    input_file: Path = INPUT_FILE,
    valid_file: Path = VALID_FILE,
    invalid_file: Path = INVALID_FILE,
) -> None:
    """
    Valida o consolidado e separa saida valida e invalida.

    :param input_file: CSV consolidado de entrada.
    :param valid_file: CSV de validos.
    :param invalid_file: CSV de inconsistencias.
    :return: None.
    """
    consolidated_df = pd.read_csv(input_file, dtype=str, encoding="utf-8-sig")
    consolidated_df["CNPJ"] = consolidated_df["CNPJ"].map(_only_digits)
    consolidated_df["RazaoSocial"] = (
        consolidated_df["RazaoSocial"].fillna("").astype(str)
    )
    consolidated_df["ValorDespesas_num"] = consolidated_df["ValorDespesas"].map(
        _parse_valor
    )

    cnpj_valid = consolidated_df["CNPJ"].map(_cnpj_is_valid)
    razao_ok = consolidated_df["RazaoSocial"].str.strip().ne("")
    valor_ok = consolidated_df["ValorDespesas_num"] > 0

    motivo = pd.Series("", index=consolidated_df.index)
    motivo = _append_reason(motivo, "CNPJ_INVALIDO", ~cnpj_valid)
    motivo = _append_reason(motivo, "RAZAO_SOCIAL_VAZIA", ~razao_ok)
    motivo = _append_reason(motivo, "VALOR_NAO_POSITIVO", ~valor_ok)
    consolidated_df["Motivo"] = motivo
    invalid = consolidated_df[consolidated_df["Motivo"] != ""].copy()
    valid = consolidated_df[consolidated_df["Motivo"] == ""].copy()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    valid.drop(columns=["ValorDespesas_num", "Motivo"]).to_csv(
        valid_file, index=False, encoding="utf-8-sig"
    )
    invalid.drop(columns=["ValorDespesas_num"]).to_csv(
        invalid_file, index=False, encoding="utf-8-sig"
    )


if __name__ == "__main__":
    validate()
    print(f"Validos: {VALID_FILE}")
    print(f"Inconsistencias: {INVALID_FILE}")
