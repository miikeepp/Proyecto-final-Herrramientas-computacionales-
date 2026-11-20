// Power Query (lenguaje M) para Excel o Power BI.
// Cambia RutaArchivo por la ruta local de data/clean/sales_clean.csv.
let
    RutaArchivo = "C:\\RUTA_DEL_PROYECTO\\data\\clean\\sales_clean.csv",
    Fuente = Csv.Document(
        File.Contents(RutaArchivo),
        [Delimiter = ",", Encoding = 65001, QuoteStyle = QuoteStyle.Csv]
    ),
    Encabezados = Table.PromoteHeaders(Fuente, [PromoteAllScalars = true]),
    Tipos = Table.TransformColumnTypes(Encabezados, {
        {"order_number", Int64.Type}, {"quantity_ordered", Int64.Type},
        {"price_each", Currency.Type}, {"sales", Currency.Type},
        {"order_date", type date}, {"year", Int64.Type}, {"month", Int64.Type},
        {"quarter", Int64.Type}, {"es_venta_realizada", type logical}
    }),
    VentasRealizadas = Table.SelectRows(Tipos, each [es_venta_realizada] = true)
in
    VentasRealizadas
