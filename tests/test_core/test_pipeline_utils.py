"""Testes para core/pipeline_utils.py."""

import pytest
import numpy as np
import pandas as pd

from core.pipeline_utils import (
    normalize_cod_item_value,
    normalize_cod_item_series,
    normalize_num_tipo_despesa,
    padronizar_desc_item,
    detectar_colunas_data_qtd_valor,
    consolidar_duplicatas_exatas,
    criar_mapa_descricao,
    preparar_colunas_temporais,
    agregar_mensal,
    agregar_trimestral,
    agregar_anual,
)


class TestNormalizeCodItemValue:
    """Testes para normalize_cod_item_value."""

    def test_int_value(self):
        """Testa normalização de inteiro."""
        assert normalize_cod_item_value(123) == "123"

    def test_float_integer(self):
        """Testa normalização de float inteiro."""
        assert normalize_cod_item_value(123.0) == "123"

    def test_string_value(self):
        """Testa normalização de string."""
        assert normalize_cod_item_value("ABC-123") == "ABC-123"

    def test_none_value(self):
        """Testa valor None."""
        assert normalize_cod_item_value(None) is None

    def test_nan_string(self):
        """Testa string 'nan'."""
        assert normalize_cod_item_value("nan") is None
        assert normalize_cod_item_value("NaN") is None

    def test_empty_string(self):
        """Testa string vazia."""
        assert normalize_cod_item_value("") is None

    def test_numpy_int(self):
        """Testa numpy integer."""
        assert normalize_cod_item_value(np.int64(456)) == "456"

    def test_numpy_float(self):
        """Testa numpy float."""
        assert normalize_cod_item_value(np.float64(789.0)) == "789"


class TestNormalizeCodItemSeries:
    """Testes para normalize_cod_item_series."""

    def test_series_mixed(self):
        """Testa série com valores mistos."""
        s = pd.Series([123, "ABC", 456.0, None, "nan"])
        result = normalize_cod_item_series(s)
        assert result.iloc[0] == "123"
        assert result.iloc[1] == "ABC"
        assert result.iloc[2] == "456"
        assert pd.isna(result.iloc[3])
        assert pd.isna(result.iloc[4])


class TestNormalizeNumTipoDespesa:
    """Testes para normalize_num_tipo_despesa."""

    def test_int_value(self):
        """Testa normalização de inteiro."""
        assert normalize_num_tipo_despesa(802) == "0802"

    def test_string_value(self):
        """Testa normalização de string."""
        assert normalize_num_tipo_despesa("0802") == "0802"

    def test_none_value(self):
        """Testa valor None."""
        assert normalize_num_tipo_despesa(None) is None


class TestPadronizarDescItem:
    """Testes para padronizar_desc_item."""

    def test_remove_extra_spaces(self):
        """Testa remoção de espaços extras."""
        df = pd.DataFrame({
            "COD_ITEM": ["A", "B"],
            "DESC_ITEM": ["Item   com   espaços", "Normal"]
        })
        result = padronizar_desc_item(df)
        assert result["DESC_ITEM"].tolist() == ["Item com espaços", "Normal"]

    def test_missing_columns(self):
        """Testa com colunas faltantes."""
        df = pd.DataFrame({"OTHER": [1, 2]})
        result = padronizar_desc_item(df)
        assert "OTHER" in result.columns


class TestDetectarColunas:
    """Testes para detectar_colunas_data_qtd_valor."""

    def test_detecta_colunas_padrao(self):
        """Testa detecção de colunas padrão."""
        df = pd.DataFrame({
            "DATA": [],
            "QUANTIDADE": [],
            "VALOR": []
        })
        col_data, col_qtd, col_valor = detectar_colunas_data_qtd_valor(df)
        assert col_data == "DATA"
        assert col_qtd == "QUANTIDADE"
        assert col_valor == "VALOR"

    def test_detecta_colunas_alternativas(self):
        """Testa detecção de colunas alternativas."""
        df = pd.DataFrame({
            "DATA_ENTREGA": [],
            "QTD": [],
            "VL_TOTAL": []
        })
        col_data, col_qtd, col_valor = detectar_colunas_data_qtd_valor(df)
        assert col_data == "DATA_ENTREGA"
        assert col_qtd == "QTD"
        assert col_valor == "VL_TOTAL"

    def test_colunas_ausentes(self):
        """Testa com colunas ausentes."""
        df = pd.DataFrame({"OUTRO": []})
        col_data, col_qtd, col_valor = detectar_colunas_data_qtd_valor(df)
        assert col_data is None
        assert col_qtd is None
        assert col_valor is None


class TestConsolidarDuplicatas:
    """Testes para consolidar_duplicatas_exatas."""

    def test_remove_duplicatas(self):
        """Testa remoção de duplicatas."""
        df = pd.DataFrame({
            "COD_ITEM": ["A", "A", "B"],
            "DESC_ITEM": ["Item A", "Item A", "Item B"],
            "QUANTIDADE": [10, 10, 20],
            "VALOR": [100, 100, 200],
            "DATA_ENTREGA": ["2024-01-01", "2024-01-01", "2024-01-02"]
        })
        result = consolidar_duplicatas_exatas(df)
        assert len(result) == 2

    def test_colunas_faltantes(self):
        """Testa com colunas faltantes."""
        df = pd.DataFrame({"COD_ITEM": ["A", "A"]})
        result = consolidar_duplicatas_exatas(df)
        assert len(result) == 2  # Não remove sem colunas necessárias


class TestCriarMapaDescricao:
    """Testes para criar_mapa_descricao."""

    def test_cria_mapa(self):
        """Testa criação de mapa."""
        df = pd.DataFrame({
            "COD_ITEM": ["A", "A", "B"],
            "DESC_ITEM": ["Item A", "Item A v2", "Item B"]
        })
        result = criar_mapa_descricao(df)
        assert len(result) == 2
        assert result.loc[result["COD_ITEM"] == "A", "DESC_ITEM"].values[0] == "Item A"


class TestPrepararColunasTemporais:
    """Testes para preparar_colunas_temporais."""

    def test_adiciona_colunas(self):
        """Testa adição de colunas temporais."""
        df = pd.DataFrame({
            "DATA": ["2024-01-15", "2024-06-20"]
        })
        result = preparar_colunas_temporais(df, "DATA")
        assert "ANO" in result.columns
        assert "ANO_MES" in result.columns
        assert "ANO_TRIM" in result.columns
        assert result["ANO"].tolist() == [2024, 2024]


class TestAgregacoes:
    """Testes para funções de agregação."""

    @pytest.fixture
    def df_teste(self):
        """DataFrame de teste."""
        df = pd.DataFrame({
            "COD_ITEM": ["A", "A", "B"],
            "ANO_MES": ["2024-01", "2024-01", "2024-02"],
            "ANO_TRIM": ["2024Q1", "2024Q1", "2024Q1"],
            "ANO": [2024, 2024, 2024],
            "QTD": [10, 20, 30]
        })
        return df

    @pytest.fixture
    def map_desc(self):
        """Mapa de descrições."""
        return pd.DataFrame({
            "COD_ITEM": ["A", "B"],
            "DESC_ITEM": ["Item A", "Item B"]
        })

    def test_agregar_mensal(self, df_teste, map_desc):
        """Testa agregação mensal."""
        result = agregar_mensal(df_teste, "QTD", map_desc)
        assert len(result) == 2
        # A em 2024-01: 10 + 20 = 30
        assert result.loc[result["COD_ITEM"] == "A", "QTD_MENSAL"].values[0] == 30

    def test_agregar_trimestral(self, df_teste, map_desc):
        """Testa agregação trimestral."""
        result = agregar_trimestral(df_teste, "QTD", map_desc)
        assert len(result) == 2

    def test_agregar_anual(self, df_teste, map_desc):
        """Testa agregação anual."""
        result = agregar_anual(df_teste, "QTD", map_desc)
        assert len(result) == 2
        # A em 2024: 10 + 20 = 30, B em 2024: 30
        total_a = result.loc[result["COD_ITEM"] == "A", "QTD_ANO"].values[0]
        assert total_a == 30
