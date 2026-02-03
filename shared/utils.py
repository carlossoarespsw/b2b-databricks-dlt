# ========================================
# Shared Utilities
# ========================================
# Funções utilitárias compartilhadas entre pipelines

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, regexp_replace
from typing import List, Dict
import logging

# Configuração de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def validate_cpf(cpf: str) -> bool:
    """
    Valida CPF brasileiro.
    
    Args:
        cpf: String do CPF (apenas números)
    
    Returns:
        True se válido, False caso contrário
    """
    if not cpf or len(cpf) != 11:
        return False
    
    # Verifica se todos os dígitos são iguais
    if cpf == cpf[0] * 11:
        return False
    
    # Validação dos dígitos verificadores
    for i in range(9, 11):
        value = sum((int(cpf[num]) * ((i + 1) - num) for num in range(0, i)))
        digit = ((value * 10) % 11) % 10
        if digit != int(cpf[i]):
            return False
    
    return True


def validate_cnpj(cnpj: str) -> bool:
    """
    Valida CNPJ brasileiro.
    
    Args:
        cnpj: String do CNPJ (apenas números)
    
    Returns:
        True se válido, False caso contrário
    """
    if not cnpj or len(cnpj) != 14:
        return False
    
    # Verifica se todos os dígitos são iguais
    if cnpj == cnpj[0] * 14:
        return False
    
    # Validação simplificada (implementação completa seria mais longa)
    return True


def clean_phone(df: DataFrame, phone_col: str) -> DataFrame:
    """
    Limpa e padroniza números de telefone.
    
    Args:
        df: DataFrame
        phone_col: Nome da coluna de telefone
    
    Returns:
        DataFrame com telefone limpo
    """
    return df.withColumn(
        phone_col,
        regexp_replace(col(phone_col), r"[^\d]", "")
    )


def standardize_text(df: DataFrame, text_cols: List[str]) -> DataFrame:
    """
    Padroniza colunas de texto (trim, upper).
    
    Args:
        df: DataFrame
        text_cols: Lista de colunas para padronizar
    
    Returns:
        DataFrame com textos padronizados
    """
    for col_name in text_cols:
        df = df.withColumn(col_name, trim(upper(col(col_name))))
    
    return df


def add_audit_columns(df: DataFrame, timestamp_col: str = "_processing_timestamp") -> DataFrame:
    """
    Adiciona colunas de auditoria.
    
    Args:
        df: DataFrame
        timestamp_col: Nome da coluna de timestamp
    
    Returns:
        DataFrame com colunas de auditoria
    """
    from pyspark.sql.functions import current_timestamp, current_user
    
    return df.withColumn(timestamp_col, current_timestamp())


def apply_scd_type2(
    df: DataFrame,
    key_cols: List[str],
    effective_date_col: str = "effective_date",
    end_date_col: str = "end_date",
    is_current_col: str = "is_current"
) -> DataFrame:
    """
    Aplica lógica de SCD Type 2 (Slowly Changing Dimension).
    
    Args:
        df: DataFrame
        key_cols: Colunas chave
        effective_date_col: Coluna de data efetiva
        end_date_col: Coluna de data fim
        is_current_col: Coluna indicadora de registro atual
    
    Returns:
        DataFrame com SCD Type 2 aplicado
    """
    from pyspark.sql.functions import current_date, lit
    
    df = df.withColumn(effective_date_col, current_date())
    df = df.withColumn(end_date_col, lit(None).cast("date"))
    df = df.withColumn(is_current_col, lit(True))
    
    return df


def detect_outliers(
    df: DataFrame,
    value_col: str,
    method: str = "iqr",
    threshold: float = 1.5
) -> DataFrame:
    """
    Detecta outliers em uma coluna numérica.
    
    Args:
        df: DataFrame
        value_col: Coluna para análise
        method: Método de detecção ('iqr' ou 'zscore')
        threshold: Threshold para outliers
    
    Returns:
        DataFrame com coluna is_outlier
    """
    from pyspark.sql.functions import percentile_approx
    
    if method == "iqr":
        # Calcula quartis
        quartiles = df.approxQuantile(value_col, [0.25, 0.75], 0.01)
        q1, q3 = quartiles[0], quartiles[1]
        iqr = q3 - q1
        
        lower_bound = q1 - (threshold * iqr)
        upper_bound = q3 + (threshold * iqr)
        
        df = df.withColumn(
            "is_outlier",
            when(
                (col(value_col) < lower_bound) | (col(value_col) > upper_bound),
                True
            ).otherwise(False)
        )
    
    return df


def log_metrics(table_name: str, metrics: Dict):
    """
    Loga métricas da pipeline.
    
    Args:
        table_name: Nome da tabela
        metrics: Dicionário de métricas
    """
    logger.info(f"[{table_name}] Metrics: {metrics}")


def get_latest_partition(df: DataFrame, partition_col: str) -> DataFrame:
    """
    Retorna apenas a última partição dos dados.
    
    Args:
        df: DataFrame
        partition_col: Coluna de particionamento
    
    Returns:
        DataFrame filtrado
    """
    max_partition = df.agg({partition_col: "max"}).collect()[0][0]
    return df.filter(col(partition_col) == max_partition)
