module DataFramesMeta

using Reexport

using MacroTools

using OrderedCollections: OrderedCollections

@reexport using DataFrames

@reexport using Chain

# Basics:
export @with,
       @subset, @subset!, @rsubset, @rsubset!,
       @orderby, @rorderby,
       @by, @combine,
       @transform, @select, @transform!, @select!,
       @rtransform, @rselect, @rtransform!, @rselect!,
       @distinct, @rdistinct, @distinct!, @rdistinct!,
       @eachrow, @eachrow!,
       @byrow, @passmissing, @astable, @kwarg,
       @based_on, @where # deprecated

const DOLLAR = raw"$"

include("parsing.jl")
include("parsing_astable.jl")
include("macros.jl")
include("linqmacro.jl")
include("eachrow.jl")

end # module