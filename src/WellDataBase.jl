module WellDataBase

import DelimitedFiles
import DataFrames
import Dates

csvheader = ["API", "WellName", "Id", "WellId", "ReportDate", "Days", "Lease", "Operator", "WellsInLease", "Field", "Formation", "TotalOil", "LeaseOilAllowable", "WellOilAllowable", "WellOil", "TotalGas", "LeaseGasAllowable", "WellGasAllowable", "WellGas", "TotalWater", "WellWater", "GOR", "ReportMonth", "ReportYear", "ReportedOperator", "ReportedFormation", "InterpretedFormation"]
csvtypes = [Int64, String, String, String, Dates.Date, Int32, Int32, String, Int32, String, String, Float32, Float32, Float32, Float32, Float32, Float32, Float32, Float32, Float32, Float32, Float32, Int32, Int32, String, String, String]

function read(datadirs::AbstractVector; location::AbstractString=".", labels=[:WellOil], skipstring=true, cvsread=["API", "ReportDate", "WellOil", "WellGas", "WellWater"], checkzero::Bool=true)
	df = DataFrames.DataFrame()
	for d in datadirs
		f = joinpath(location, d, d * "-Production.csv")
		if !isfile(f)
			@warn("File $f is missing!")
			continue
		end
		@info("File: $f")
		a, h = DelimitedFiles.readdlm(f, ','; header=true)
		dfl = DataFrames.DataFrame()
		for i = 1:size(a, 2)
			s = Symbol(csvheader[i])
			ism = a[:, i] .== ""
			if csvheader[i] in cvsread
				@info("Column $i: $(csvheader[i]) Type: $(csvtypes[i]) Number of missing entries: $(sum(ism))")
				if csvtypes[i] == Dates.Date
					dfl[!, s] = Dates.Date.(a[:, i], "mm/dd/yyyy HH:MM:SS")
				elseif !skipstring && csvtypes[i] == String
					iwelldates = typeof.(a[:, i]) .== Int64
					a[iwelldates, i] .= string.(a[iwelldates, i])
					dfl[!, s] = a[:, i]
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
			else
				@info("Column $i: $(csvheader[i]) Type: $(csvtypes[i]) Number of missing entries: $(sum(ism)) SKIPPED!")
			end
		end
		df = vcat(df, dfl)
	end

	if size(df, 1) == 0
		@warn("No data provided to read!")
		return nothing
	end

	api = unique(sort(df[!, :API]))

	@info("Number of wells: $(length(api))")

	startdate = maximum(df[!, :ReportDate])
	enddate = minimum(df[!, :ReportDate])
	recordlength = 0
	longwell = 0
	longwellc = 0
	goodwells = falses(length(api))
	for (i, w) in enumerate(api)
		iwell = df[!, :API] .== w
		innvol = falses(sum(iwell))
		gw = false
		for l in labels
			p = df[!, l][iwell]
			if sumnan(p) > 0
				gw = true
				if checkzero
					innvol .|= p .> 0
				else
					innvol .|= .!isnan.(p)
				end
			end
		end
		goodwells[i] = gw
		if sum(innvol) > 0
			welldates = df[!, :ReportDate][iwell][innvol]
			dmin = minimum(welldates)
			dmax = maximum(welldates)
			rl = length(dmin:Dates.Month(1):dmax)
			if recordlength < rl
				recordlength = rl
				longwell = w
				longwellc = i
			end
			startdate = min(startdate, dmin)
			enddate = max(enddate, dmax)
		end
	end
	@info("Number of good wells: $(sum(goodwells))")

	@info("Record start date: $(startdate)")
	@info("Record end  date: $(enddate)")
	@info("Max record length: $(recordlength) months")
	@info("Long well: $(longwell) ($(longwellc))")

	dates = startdate:Dates.Month(1):enddate

	return df, api, goodwells, recordlength, dates
end

function read_header(datadirs::AbstractVector; location::AbstractString=".")
	df_header = DataFrames.DataFrame()
	for d in datadirs
		f = joinpath(location, d, d * "-Header.csv")
		if !isfile(f)
			@warn("File $f is missing!")
			continue
		end
		@info("File: $f")
		a, h = DelimitedFiles.readdlm("data/eagleford-play-20191008/$(d)/$(d)-Header.csv", ','; header=true)
		dfl = DataFrames.DataFrame()
		for i = 1:size(a, 2)
			ism = a[:, i] .== ""
			processed = false
			if h[i] == "API"
				processed = true
				a[ism, i] .= 0
				dfl[!, :API] = convert.(Int64, a[:, i])
			elseif h[i] == "BHLatitude"
				processed = true
				a[ism, i] .= NaN
				dfl[!, :Lat] = convert.(Float32, a[:, i])
			elseif h[i] == "BHLongitude"
				processed = true
				a[ism, i] .= NaN
				dfl[!, :Lon] = convert.(Float32, a[:, i])
			elseif h[i] == "TrueVerticalDepth"
				processed = true
				a[ism, i] .= NaN
				dfl[!, :Depth] = convert.(Float32, a[:, i])
			elseif h[i] == "WellType"
				processed = true
				a[ism, i] .= ""
				dfl[!, :WellType] = uppercase.(convert.(String, a[:, i]))
			elseif h[i] == "PrimaryFormation"
				processed = true
				a[ism, i] .= ""
				dfl[!, :Formation] = uppercase.(convert.(String, a[:, i]))
			elseif h[i] == "ReportedCurrentOperator"
				processed = true
				a[ism, i] .= ""
				dfl[!, :Operator] = uppercase.(convert.(String, a[:, i]))
			elseif h[i] == "ReportedWellboreProfile"
				processed = true
				a[ism, i] .= ""
				dfl[!, :Orientation] = uppercasefirst.(lowercase.(convert.(String, a[:, i])))
			end
			@info "Col $i $(h[i]) --> $(processed)"
		end
		df_header = vcat(df_header, dfl)
	end
	return df_header
end

function sumnan(X; dims=nothing, kw...)
	if dims == nothing
		return sum(X[.!isnan.(X)]; kw...)
	else
		count = .*(size(X)[vec(collect(dims))]...)
		I = isnan.(X)
		X[I] .= 0
		sX = sum(X; dims=dims, kw...)
		X[I] .= NaN
		sI = sum(I; dims=dims, kw...)
		sX[sI.==count] .= NaN
		return sX
	end
end

end