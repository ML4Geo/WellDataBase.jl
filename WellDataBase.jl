module WellDataBase

import DelimitedFiles
using DataFrames: DataFrames, DataFrame, groupby, combine
using Underscores: @_
import Dates
import CSV

csvheader = ["API", "WellName", "Id", "WellId", "ReportDate", "Days", "Lease", "Operator", "WellsInLease", "Field", "Formation", "TotalOil", "LeaseOilAllowable", "WellOilAllowable", "WellOil", "TotalGas", "LeaseGasAllowable", "WellGasAllowable", "WellGas", "TotalWater", "WellWater", "GOR", "ReportMonth", "ReportYear", "ReportedOperator", "ReportedFormation", "InterpretedFormation"]
csvtypes = [Int64, String, String, String, Dates.Date, Int32, Int32, String, Int32, String, String, Float32, Float32, Float32, Float32, Float32, Float32, Float32, Float32, Float32, Float32, Float32, Int32, Int32, String, String, String]

sumnan(xs) = sum(filter(!isnan, xs))

function read(datadirs=["csv-201908102241", "csv-201908102238", "csv-201908102239"]; location="data/eagleford-play-20191008")
	df = DataFrames.DataFrame()
	for d in datadirs
		a, h = DelimitedFiles.readdlm(joinpath(location, d, d * "-Production.csv"), ','; header=true)
		dfl = DataFrames.DataFrame()
		for i = 1:size(a, 2)
			s = Symbol(csvheader[i])
			ism = a[:, i] .== ""
			@info "Column " i csvheader[i] csvtypes[i] sum(ism)
			if csvtypes[i] == Dates.Date
				dfl[!, s] = Dates.Date.(a[:, i], Dates.dateformat"mm/dd/yyyy HH:MM:SS")
			elseif csvtypes[i] == String
				iwelldates = typeof.(a[:, i]) .== Int64
				a[iwelldates, i] .= string.(a[iwelldates, i])
			else
				if sum(ism) > 0
					if csvtypes[i] == Float32
						a[ism, i] .= NaN
						iwelldates = a[:, i] .< 0
						a[iwelldates, i] .= 0
					elseif csvtypes[i] == Int32
						a[ism, i] .= 0
					end
				end
				dfl[!, s] = convert.(csvtypes[i], a[:, i])
			end
		end
		df = vcat(df, dfl)
	end

	api = unique(sort(df[!, :API]))

	goodwells = falses(length(api))
	for (i, w) in enumerate(api)
		iwell = findall((in)(w), df[!, :API])
		oil = df[!, :WellOil][iwell]
		gas = df[!, :WellGas][iwell]
		if sumnan(oil) > 0 || sumnan(gas) > 0
			goodwells[i] = true
		end
	end

	startdate = maximum(df[!, :ReportDate])
	enddate = minimum(df[!, :ReportDate])
	recordlength = 0
	for w in api[goodwells]
		iwell = findall((in)(w), df[!, :API])
		oil = df[!, :WellOil][iwell]
		gas = df[!, :WellGas][iwell]
		water = df[!, :WellWater][iwell]
		innvol = .!isnan.(oil) .| .!isnan.(gas) .| .!isnan.(water)
		welldates = df[!, :ReportDate][iwell][innvol]
		dmin = minimum(welldates)
		dmax = maximum(welldates)
		recordlength = max(recordlength, length(dmin:Dates.Month(1):dmax))
		startdate = min(startdate, dmin)
		enddate = max(enddate, dmax)
	end

	dates = startdate:Dates.Month(1):enddate

	return df, api, goodwells, dates
end

function well_summary(oil, gas, water, report_date)
    welldates = report_date[.!ismissing.(oil) .|
                            .!ismissing.(gas) .|
                            .!ismissing.(water)]
    (WellOil_sum=sum(skipmissing(oil)),
     WellGas_sum=sum(skipmissing(gas)),
     StartDate=isempty(welldates) ? missing : minimum(welldates),
     EndDate=isempty(welldates) ? missing : maximum(welldates))
end

function read2(datadirs=["csv-201908102241", "csv-201908102238", "csv-201908102239"]; location="data/eagleford-play-20191008")
    df = DataFrames.DataFrame()
    for d in datadirs
        path = joinpath(location, d, d * "-Production.csv")
        dfl = DataFrame(CSV.File(path, types=csvtypes,
                                 dateformat=Dates.dateformat"mm/dd/yyyy HH:MM:SS"))
        @info "Loaded data" path summary(dfl)
        for (i,coltype) in enumerate(csvtypes)
            if coltype == Float32
                dfl[:, i] = max.(dfl[!, i], 0.0f0)
            end
        end
        df = vcat(df, dfl)
    end

    goodwell_summary = @_ df |>
        groupby(__, :API) |>
        combine([:WellOil,:WellGas,:WellWater,:ReportDate]=>well_summary, __) |>
        filter(_.WellOil_sum > 0 || _.WellGas_sum > 0, __)

    startdate = minimum(skipmissing(goodwell_summary.StartDate))
    enddate = maximum(skipmissing(goodwell_summary.EndDate))

    dates = startdate:Dates.Month(1):enddate

    return df, goodwell_summary, dates
end


end
