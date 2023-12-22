module DataFramesMeta

using Reexport

using MacroTools

using OrderedCollections: OrderedCollections

@reexport using TableMetadataTools

@reexport using DataFrames

@reexport using Chain

using DataFrames.PrettyTables

# Basics:
export @with,
       @subset, @subset!, @rsubset, @rsubset!,
       @orderby, @rorderby,
       @by, @combine,
       @rename, @rename!,
       @transform, @select, @transform!, @select!,
       @rtransform, @rselect, @rtransform!, @rselect!,
       @distinct, @rdistinct, @distinct!, @rdistinct!,
       @eachrow, @eachrow!,
       @byrow, @passmissing, @astable, @kwarg,
       @addlabel, @addnote,
       @based_on, @where # deprecated

const DOLLAR = raw"$"

include("parsing.jl")
include("parsing_astable.jl")
include("macros.jl")
include("linqmacro.jl")
include("eachrow.jl")
include("metadata.jl")

end # module