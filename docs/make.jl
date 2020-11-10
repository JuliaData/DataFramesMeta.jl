using Documenter, DataFramesMeta

makedocs(
	modules = [DataFramesMeta],
	sitename = "DataFramesMeta Documentation",
	pages = Any[
		"Introduction" => "index.md",
		"API" => "api/api.md"])