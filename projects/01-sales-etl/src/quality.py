"""Validaciones de calidad de datos para 01-sales-etl."""


def assert_no_nulls(df, columns):
    """Lanza un error si alguna de las columnas indicadas contiene nulos."""
    # TODO
    raise NotImplementedError


def assert_unique(df, columns):
    """Lanza un error si la combinacion de columnas no es unica (p.ej. order_id)."""
    # TODO
    raise NotImplementedError


def assert_positive(df, column):
    """Lanza un error si la columna contiene valores <= 0 (p.ej. total_price)."""
    # TODO
    raise NotImplementedError


def validate(df):
    """Punto de entrada unico: encadena todas las comprobaciones de 01-sales-etl."""
    # TODO: assert_no_nulls(df, [...]) / assert_unique(df, [...]) / assert_positive(df, "total_price")
    pass


if __name__ == "__main__":
    pass
