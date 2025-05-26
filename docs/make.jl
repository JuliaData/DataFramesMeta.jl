using Documenter, DataFramesMeta

makedocs(
    modules = [DataFramesMeta],
    sitename = "DataFramesMeta",
    format = Documenter.HTML(
        canonical = "https://juliadata.github.io/DataFramesMeta.jl/stable/"
    ),
    pages = Any[
        "Introduction" => "index.md",
        "Tutorial for coming from dplyr" => "dplyr.md",
        "API" => "api/api.md",
    ],
    warnonly = [:missing_docs],
)

deploydocs(
    repo = "github.com/JuliaData/DataFramesMeta.jl.git",
    target = "build",
    deps = nothing,
    make = nothing,
)
