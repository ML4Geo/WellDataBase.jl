module ReadWellData_All
import DelimitedFiles
import DataFrames
import Dates
import CSV
import Statistics
import MLJModels
import Flux

csvheader = ["API", "WellName", "Id", "WellId", "ReportDate", "Days", "Lease", "Operator", "WellsInLease", "Field", "Formation", "TotalOil", "LeaseOilAllowable", "WellOilAllowable", "WellOil", "TotalGas", "LeaseGasAllowable", "WellGasAllowable", "WellGas", "TotalWater", "WellWater", "GOR", "ReportMonth", "ReportYear", "ReportedOperator", "ReportedFormation", "InterpretedFormation",
"CasingType", "WellboreSize", "UpperSetDepth", "LowerSetDepth", "CementSacks", "CasingSize",
"UpperPerf", "LowerPerf", "UpperPerfTVD", "LowerPerfTVD",
"TopDepth",
"BHLatitude", "BHLongitude","TrueVerticalDepth", "LateralLength", "ThermalMaturity", "PrimaryFormation",
"WaterInjection", "GasInjection", "C02Injection", "TubingPressure",
"StimDate", "TotalProppantMass",
"TestDate", "TestOil", "DailyOil", "OilGravity", "TestGas",
"TubingSize", "TubingUpperDepth", "PackerDepth"]

csvtypes = [Int64, String, String, String, Dates.Date, Int32, Int32, String, Int32, String, String, Float32, Float32, Float32, Float32, Float32, Float32, Float32, Float32, Float32, Float32, Float32, Int32, Int32, String, String, String,
String, Float32, Float32, Float32, Float32, Float32,
Float32, Float32, Float32, Float32,
Float32,
Float32, Float32, Float32, Float32, String, String,
Float32, Float32, Float32, Float32,
Dates.Date, Float32,
Dates.Date, Float32, Float32, Float32, Float32,
Float32, Float32, Float32]
files = ["Casing", "Completion", "Formation", "Header", "Injection", "Stimulation", "Test", "TubingAndPacking"]
#Production, "Completion", "Directional", "Formation", "FracIngredient", "FracStage", "Header", "History", "Injection", "Perf", "Permit", "ProductionSummary", "Stimulation", "Test", "TubingAndPacking", "WaterChemical", "WaterSummary"]

cvsread=["API", "Id", "WellId", "CasingType", "CementSacks", "UpperSetDepth", "LowerSetDepth",
"WellboreSize", "CasingSize", "UpperPerf", "LowerPerf", "UpperPerfTVD", "LowerPerfTVD", "TopDepth","BHLatitude", "BHLongitude",
"TrueVerticalDepth", "LateralLength", "ThermalMaturity", "PrimaryFormation", "WaterInjection", "GasInjection", "C02Injection", "TubingPressure",
"StimDate", "TotalProppantMass","TestDate", "TestOil", "DailyOil", "OilGravity", "TestGas"]


function read_static(datadirs::AbstractVector; location::AbstractString=".", labels=[:WellOil], skipstring=false,
	cvsread=["API","CasingType", "CementSacks", "UpperSetDepth", "LowerSetDepth",
"WellboreSize", "CasingSize", "UpperPerf", "LowerPerf", "UpperPerfTVD", "LowerPerfTVD", "TopDepth", "BHLatitude", "BHLongitude",
"TrueVerticalDepth", "LateralLength", "ThermalMaturity", "PrimaryFormation", "WaterInjection", "GasInjection", "C02Injection", "TubingPressure",
"StimDate", "TotalProppantMass","TestDate", "TestOil", "DailyOil", "OilGravity", "TestGas"]
, checkzero::Bool=true, downselect::AbstractVector=[])
	df_header = read_header(datadirs; location=location)
	df = DataFrames.DataFrame()
	for file in files
		dfx = DataFrames.DataFrame()
		gd = DataFrames.DataFrame()
		for d in datadirs
			f = joinpath(location, d, d * "-" * file * ".csv")
			if !isfile(f)
				@warn("File $f is missing!")
				continue
			end
			@info("File: $f")
			a, h = DelimitedFiles.readdlm(f, ','; header=true)
			dfl = DataFrames.DataFrame()
			for i = 1:size(a, 2)
				s = Symbol(h[i])
				ism = a[:, i] .== ""
				if h[i] in cvsread
					@info("Column: $s")
					j = findall(x -> x==h[i], cvsread)
					k = findall(x -> x==h[i], csvheader)
					@info("Column $i: $(h[i]) Type: $(csvtypes[k]) Number of missing entries: $(sum(ism))")
					if csvtypes[k][1] == Dates.Date && file == "Production"
						dfl[!, s] = Dates.Date.(a[:, i], "mm/dd/yyyy HH:MM:SS")
					elseif !skipstring && csvtypes[k][1] == String
						iwelldates = typeof.(a[:, i]) .== Int64
						a[iwelldates, i] .= string.(a[iwelldates, i])
						dfl[!, s] = a[:, i]
					elseif csvtypes[k][1] == Dates.Date && file != "Production"
						# Deal with Transient Data outside of Production files
					else
						if sum(ism) > 0
							if csvtypes[k][1] == Float32
								a[ism, i] .= NaN
								iwelldates = a[:, i] .< 0
								a[iwelldates, i] .= 0
							elseif csvtypes[k][1] == Int32
								a[ism, i] .= 0
							end
						end
						dfl[!, s] = convert.(csvtypes[k][1], a[:, i])
					end
				else
					#@info("Column $i: $(csvheader[i]) Type: $(csvtypes[i]) Number of missing entries: $(sum(ism)) SKIPPED!")
				end
			end
			dfx = vcat(dfx, dfl)
		end
		if size(dfx, 2) > 1
			if file == files[1]
				df = hcat(df,  unique(dfx[:,[:API]]))
				gd = DataFrames.groupby(dfx, :API)
				df_numerical = [i for i in names(dfx) if Base.nonmissingtype(eltype(dfx[!,i])) <: Number]
				df_string = [i for i in names(dfx) if Base.nonmissingtype(eltype(dfx[!,i])) == Any]
				gdx =DataFrames.DataFrame()
				for column in df_numerical[2:end]
					if length(findall(x -> occursin(column, x), names(df))) == 0
					gdx = DataFrames.combine(gd, column => Statistics.mean)
					df = DataFrames.leftjoin(df, gdx, on= :API, makeunique=true)
					end
				end
				for column in df_string[1:end]
					if length(findall(x -> occursin(column, x), names(df))) == 0
					gdx = DataFrames.combine(gd, column => Statistics.first)
					df = DataFrames.leftjoin(df, gdx, on= :API, makeunique=true)
					end
				end
			elseif file != files[1]
				gd = DataFrames.groupby(dfx, :API)
				df_numerical = [i for i in names(dfx) if Base.nonmissingtype(eltype(dfx[!,i])) <: Number]
				df_string = [i for i in names(dfx) if Base.nonmissingtype(eltype(dfx[!,i])) == Any]
				gdx =DataFrames.DataFrame()
				for column in df_numerical[2:end]
					if length(findall(x -> occursin(column, x), names(df))) == 0
					gdx = DataFrames.combine(gd, column => Statistics.first)
					df = DataFrames.leftjoin(df, gdx, on= :API, makeunique=true)
					end
				end
				for column in df_string[1:end]
					if length(findall(x -> occursin(column, x), names(df))) == 0
					gdx = DataFrames.combine(gd, column => Statistics.first)
					df = DataFrames.leftjoin(df, gdx, on= :API, makeunique=true)
					end
				end
			end
		end
	end
	# One hot encode categorical data
	OneHot = MLJModels.OneHotEncoder()
	df_string = [i for i in names(df) if Base.nonmissingtype(eltype(df[!,i])) == SubString{String}]
	for column in df_string[1:end]
		if occursin("Id", column)
			DataFrames.select!(df, DataFrames.Not(column))
		else
		new_cols = Flux.onehotbatch(df[!, column], unique(df[!, column])) |> transpose
		col_names = repeat([column], size(new_cols)[2])
			for z = 1:size(col_names)[1]
				col_names[z] = col_names[z]*'_'*string(z)
			end
		df_dummy = DataFrames.DataFrame(new_cols)
		DataFrames.rename!(df_dummy, col_names)
		df_dummy = convert.(Int32, df_dummy[!,:])
		df = hcat(df, df_dummy)
		DataFrames.select!(df, DataFrames.Not(column))
		end
	end
	if size(df, 1) == 0
		@warn("No data provided to read!")
		return nothing
	end

	api = unique(sort(df[!, :API]))

	@info("Number of wells: $(length(api))")

	longwell = 0
	longwellc = 0
	goodwells = falses(length(api))
	for (i, w) in enumerate(api)
		iwell = df[!, :API] .== w
		innvol = falses(sum(iwell))
		gw = false
		if length(downselect) > 0
			ig = indexin(w, df_header[!, :API])[1]
			if ig == nothing
				lgw = false
			else
				lgw = true
				for d in downselect
					if df_header[ig, d[1]] != d[2]
						lgw = false
					end
				end
			end
			gw = lgw
		end
		goodwells[i] = gw
	end
	@info("Number of good wells: $(sum(goodwells))")
	@info("Long well: $(longwell) ($(longwellc))")
	df_header = df_header[indexin(api[goodwells], df_header[!, :API]),:]
	df = df[findall((in)(api[goodwells]), df[!, :API]), :]
	return df, df_header, api[goodwells]
end



function read_transient(datadirs::AbstractVector; location::AbstractString=".", labels=[:WellOil], skipstring=true, cvsread=["API", "ReportDate", "WellOil", "WellGas", "WellWater"], checkzero::Bool=true, downselect::AbstractVector=[])
	df_header = read_header(datadirs; location=location)
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
		if length(downselect) > 0
			ig = indexin(w, df_header[!, :API])[1]
			if ig == nothing
				lgw = false
			else
				lgw = true
				for d in downselect
					if df_header[ig, d[1]] != d[2]
						lgw = false
					end
				end
			end
			gw = lgw
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

	df_header = df_header[indexin(api[goodwells], df_header[!, :API]),:]
	df = df[findall((in)(api[goodwells]), df[!, :API]), :]

	return df, df_header, api[goodwells], recordlength, dates
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
