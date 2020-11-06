module WellDataBase

import DelimitedFiles
import DataFrames
import Dates

csvheader = ["API", "WellName", "Id", "WellId", "ReportDate", "Days", "Lease", "Operator", "WellsInLease", "Field", "Formation", "TotalOil", "LeaseOilAllowable", "WellOilAllowable", "WellOil", "TotalGas", "LeaseGasAllowable", "WellGasAllowable", "WellGas", "TotalWater", "WellWater", "GOR", "ReportMonth", "ReportYear", "ReportedOperator", "ReportedFormation", "InterpretedFormation"]
csvtypes = [Int64, String, String, String, Dates.Date, Int32, Int32, String, Int32, String, String, Float32, Float32, Float32, Float32, Float32, Float32, Float32, Float32, Float32, Float32, Float32, Int32, Int32, String, String, String]

function read(datadirs::AbstractVector; location::AbstractString=".", labels=[:WellOil], skipstring=true, cvsread=["API", "ReportDate", "WellOil", "WellGas", "WellWater"])
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
				@info("Column $i: $(csvheader[i]) Type: $(csvtypes[i]) Number of missing entries: $(sum(ism))")
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
	goodwells = falses(length(api))
	for (i, w) in enumerate(api)
		# iwell = findall((in)(w), df[!, :API])
		iwell = df[!, :API] .== w
		innvol = falses(sum(iwell))
		gw = false
		for l in labels
			p = df[!, l][iwell]
			if sumnan(p) > 0
				gw = true
				innvol .|= .!isnan.(p)
			end
		end
		goodwells[i] = gw
		if sum(innvol) > 0
			welldates = df[!, :ReportDate][iwell][innvol]
			dmin = minimum(welldates)
			dmax = maximum(welldates)
			recordlength = max(recordlength, length(dmin:Dates.Month(1):dmax))
			startdate = min(startdate, dmin)
			enddate = max(enddate, dmax)
		end
	end
	@info("Number of good wells: $(sum(goodwells))")

	@info("Record start date: $(startdate)")
	@info("Record end  date: $(enddate)")
	@info("Max record length: $(recordlength) months")

	dates = startdate:Dates.Month(1):enddate

	return df, api, goodwells, recordlength, dates
end

function create_production_matrix(df, api, goodwells, dates; label=:WellOil)
	oilm = Array{Float32}(undef, length(dates), sum(goodwells))
	oilm .= NaN32
	for (i, w) in enumerate(api[goodwells])
		iwell = findall((in)(w), df[!, :API])
		oil = df[!, label][iwell]
		innoil = .!isnan.(oil)
		welldates = df[!, :ReportDate][iwell][innoil]
		iwelldates = indexin(welldates, collect(dates))
		oilm[iwelldates, i] .= 0
		for (a, b) in enumerate(oil[innoil])
			oilm[iwelldates[a], i] += b
		end
	end
	return oilm
end

function create_production_matrix_shifted(df, api, goodwells, recordlength, dates; label=:WellOil)
	oils = Array{Float32}(undef, recordlength, sum(goodwells))
	oils .= NaN32
	startdates = Array{Dates.Date}(undef, sum(goodwells))
	enddates = Array{Dates.Date}(undef, sum(goodwells))
	totaloil = Array{Float32}(undef, sum(goodwells))
	for (i, w) in enumerate(api[goodwells])
		iwell = findall((in)(w), df[!, :API])
		welldates = df[!, :ReportDate][iwell]
		isortedwelldates = sortperm(welldates)
		oil = df[!, label][iwell][isortedwelldates]
		innoil = .!isnan.(oil)
		totaloil[i] = sum(oil[innoil])
		if totaloil[i] == 0
			@warn("Well $w: has zero production ($(string(label)))!")
			ioilfirst = findfirst(i->i>0, innoil)
			ioillast = findlast(i->i>0, innoil)
		else
			ioilfirst = findfirst(i->i>0, oil[innoil])
			ioillast = findlast(i->i>0, oil[innoil])
		end
		startdates[i] = welldates[isortedwelldates][innoil][ioilfirst]
		enddates[i] = welldates[isortedwelldates][innoil][ioillast]
		iwelldates = indexin(welldates[isortedwelldates][innoil], collect(dates))
		iwelldates2 = iwelldates[ioilfirst:end] .- iwelldates[ioilfirst] .+ 1
		oils[iwelldates2, i] .= 0
		for (a, b) in enumerate(ioilfirst:length(oil[innoil]))
			oils[iwelldates2[a], i] += oil[innoil][b]
		end
		if totaloil[i] != sumnan(oils[:, i])
			@warn("Well $w (column $i): something is very wrong!")
			@show totaloil[i]
			@show sumnan(oils[:, i])
			@show sum(oils[:, i] .> 0)

			@show oil[innoil]

			@show iwelldates2
			@show dates[iwelldates]
			@show ioilfirst:length(oil[innoil])

			oils[iwelldates2, i] .= 0
			for (a, b) in enumerate(ioilfirst:length(oil[innoil]))
				@show (a, b)
				@show dates[iwelldates][a]
				@show iwelldates2[a]
				@show oil[innoil][b]
				oils[iwelldates2[a], i] += oil[innoil][b]
				@show oils[iwelldates2[a], i]
			end

			@show sumnan(oils[:, i])
			@show sum(oils[:, i] .> 0)
		end
	end
	return oils, startdates, enddates, totaloil
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