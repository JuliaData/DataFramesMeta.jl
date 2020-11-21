using Documenter, DataFramesMeta

makedocs(
	modules = [DataFramesMeta],
	sitename = "DataFramesMeta Documentation",
	format = Documenter.HTML(
		canonical = "https://juliadata.github.io/DataFramesMeta.jl/stable/"
	),
	pages = Any[
		"Introduction" => "index.md",
		"API" => "api/api.md"])

deploydocs(
    repo = "github.com/JuliaData/DataFramesMeta.jl.git",
    target = "build",
    deps = nothing,
    make = nothing,
)
