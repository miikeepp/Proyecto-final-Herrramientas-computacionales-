// Power Query (lenguaje M) de la tabla sales_clean en Power BI.
// Uso: Transformar datos > clic derecho en sales_clean > Editor avanzado > pegar esto.
// Solo cambie RutaProyecto por la carpeta del proyecto en su PC.
let
    RutaProyecto = "C:\Users\qalej\Documents\Proyecto-final-Herrramientas-computacionales-\",
    Origen = Csv.Document(
        File.Contents(RutaProyecto & "data\clean\sales_clean.csv"),
        // QuoteStyle.Csv respeta las comillas: "AV Stores, Co." queda en una sola columna
        [Delimiter = ",", Encoding = 65001, QuoteStyle = QuoteStyle.Csv]
    ),
    Encabezados = Table.PromoteHeaders(Origen, [PromoteAllScalars = true]),
    // "en-US": el CSV usa punto decimal, sin importar la configuración regional de Windows
    Tipos = Table.TransformColumnTypes(Encabezados, {
        {"order_number", Int64.Type}, {"quantity_ordered", Int64.Type},
        {"price_each", type number}, {"order_line_number", Int64.Type},
        {"sales", type number}, {"order_timestamp", type date}, {"status", type text},
        {"source_quarter_id", Int64.Type}, {"source_month_id", Int64.Type}, {"source_year_id", Int64.Type},
        {"product_line", type text}, {"msrp", type number}, {"product_code", type text},
        {"customer_name", type text}, {"phone", type text}, {"address_line_1", type text},
        {"address_line_2", type text}, {"city", type text}, {"state", type text},
        {"postal_code", type text}, {"country", type text}, {"territory", type text},
        {"contact_last_name", type text}, {"contact_first_name", type text}, {"deal_size", type text},
        {"invalid_order_number", type logical}, {"invalid_order_line_number", type logical},
        {"invalid_quantity", type logical}, {"invalid_price_each", type logical},
        {"invalid_sales", type logical}, {"invalid_order_date", type logical},
        {"invalid_msrp", type logical}, {"has_quality_error", type logical},
        {"es_venta_realizada", type logical}, {"order_date", type date},
        {"year", Int64.Type}, {"month", Int64.Type}, {"quarter", Int64.Type},
        {"discount_pct", type number}, {"estimated_cost", type number},
        {"profit", type number}, {"margin_pct", type number}
    }, "en-US")
in
    Tipos
