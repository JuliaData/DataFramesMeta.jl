module DataFramesMeta

using Reexport

@reexport using DataFrames

# Basics:
export @with, @where, @orderby, @transform, @by, @combine, @select, @eachrow,
       @transform!, @select!,
       @byrow, @byrow!, @based_on # deprecated


global const DATAFRAMES_GEQ_22 = isdefined(DataFrames, :pretty_table) ? true : false

include("linqmacro.jl")
include("eachrow.jl")
include("parsing.jl")
include("macros.jl")

end # module