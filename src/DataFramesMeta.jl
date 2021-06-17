module DataFramesMeta

using Reexport

@reexport using DataFrames

# Basics:
export @with, @subset, @orderby, @transform, @by, @combine, @select,
       @transform!, @select!,
       @eachrow, @eachrow!,
       @byrow,
       @based_on, @where # deprecated


global const DATAFRAMES_GEQ_22 = isdefined(DataFrames, :pretty_table) ? true : false

include("parsing.jl")
include("macros.jl")
include("linqmacro.jl")
include("eachrow.jl")

end # module