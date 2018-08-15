package au.com.nig.dr.util

import com.google.api.services.bigquery.model.TableFieldSchema
import com.google.api.services.bigquery.model.TableRow
import com.google.api.services.bigquery.model.TableSchema
import com.google.common.collect.Lists

object BigQueryUtil {
    private val dsCols = Lists.newArrayList<String>("DateTenancyCommenced", "DateLodgement", "BondAmount",
            "PremisesWeeklyRent", "PremisesDwellingType", "NumberBedrooms", "Premises_Postcode", "Premises_Suburb")

    fun getTableSchema(): TableSchema {
        val tableFields = java.util.ArrayList<TableFieldSchema>()
        dsCols.stream().forEach { col ->
            val tableField = TableFieldSchema().apply {
                name = col
                type = "STRING"
            }
            tableFields.add(tableField)
        }
        return TableSchema().setFields(tableFields)
    }

    fun formatBigQueryRow(values: List<String>): TableRow {
        val row = TableRow()
        val max = if (dsCols.size <= values.size) dsCols.size else values.size
        for (i in 0 until max) {
            row.set(dsCols[i], values[i])
        }
        return row
    }
}