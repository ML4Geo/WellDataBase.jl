# WellDataBase.jl


Example:

```
df, api, goodwells, recordlength, dates = WellDataBase.read(["csv-201908102241", "csv-201908102238", "csv-201908102239"]; location="data/eagleford-play-20191008")
oils, startdates, enddates, totaloil = WellDataBase.create_production_matrix_shifted(df, api, goodwells, recordlength, dates)
```
