module TestKW

using Test
using DataFrames
using DataFramesMeta
using Statistics

const ≅ = isequal

# @subset
# skipmissing, view, ungroup

# @rsubset
# skipmissing, view, ungroup

# @subset!
# skipmissing, view, ungroup

# @rsubset!
# skipmissing, view, ungroup

# @orderby # Not added

# @rorderby # Not added

# @select
# copycols, renamecols (not relevant)
# keepkeys, ungroup

# @rselect
# copycols, renamecols (not relevant)
# keepkeys, ungroup

# @select!
# renamecols (not relevant), ungroup

# @rselect!
# renamecols (not relevant), ungroup

# @transform
# copycols, renamecols (not relevant)
# ungroup

# @rtransform
# copycols, renamecols (not relevant)
# ungroup

# @transform!
# renamecols (not relevant), ungroup

# @rtransform!
# renamecols (not relevant), ungroup

# @combine
# renamecols (not relevant), keepkeys,
# ungroup

# @by
# renamecols (not relevant), keepkeys,
# ungroup

end # module