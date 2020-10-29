module WellDataBase

import DelimitedFiles
import DataFrames
import Dates

csvheader = ["API", "WellName", "Id", "WellId", "ReportDate", "Days", "Lease", "Operator", "WellsInLease", "Field", "Formation", "TotalOil", "LeaseOilAllowable", "WellOilAllowable", "WellOil", "TotalGas", "LeaseGasAllowable", "WellGasAllowable", "WellGas", "TotalWater", "WellWater", "GOR", "ReportMonth", "ReportYear", "ReportedOperator", "ReportedFormation", "InterpretedFormation"]
csvtypes = [Int64, String, String, String, Dates.Date, Int32, Int32, String, Int32, String, String, Float32, Float32, Float32, Float32, Float32, Float32, Float32, Float32, Float32, Float32, Float32, Int32, Int32, String, String, String]

function read(datadirs=["csv-201908102241", "csv-201908102238", "csv-201908102239"]; location="data/eagleford-play-20191008")
	df = DataFrames.DataFrame()
	for d in datadirs
		@show location
		a, h = DelimitedFiles.readdlm(joinpath(location, d, d * "-Production.csv"), ','; header=true)
		dfl = DataFrames.DataFrame()
		for i = 1:size(a, 2)
			s = Symbol(csvheader[i])
			ism = a[:, i] .== ""
			@info "Column " i csvheader[i] csvtypes[i] sum(ism)
			if csvtypes[i] == Dates.Date
				dfl[!, s] = Dates.Date.(a[:, i], "mm/dd/yyyy HH:MM:SS")
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
		if NMFk.sumnan(oil) > 0 || NMFk.sumnan(gas) > 0
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

end