module TestKW

using Test
using DataFrames
using DataFramesMeta
using Statistics

const ≅ = isequal

df = DataFrame(a = [1, 1, 2, 2], b = [3, 4, 5, missing])
gd = groupby(df, :a)

# @subset
# skipmissing, view, ungroup
df2 = @subset(df, :a .== 1; view = true)
@test df2 ≅ view(df, df.a .== 1, :)
@test_throws ArgumentError @subset(df, :b .== 3; skipmissing = false)
gd2 = @subset(gd, [true, true]; ungroup = false)
@test gd2 ≅ groupby(df, :a)

# @rsubset
# skipmissing, view, ungroup
df2 = @rsubset(df, :a == 1; view = true)
@test df2 ≅ view(df, df.a .== 1, :)
@test_throws ArgumentError @rsubset(df, :b == 3; skipmissing = false)
gd2 = @rsubset(gd, first(true); ungroup = false)
@test gd2 ≅ groupby(df, :a)

# @subset!
# skipmissing, ungroup
@test_throws ArgumentError @subset!(copy(df), :b .== 3; skipmissing = false)
gd2 = @subset!(deepcopy(gd), [true, true]; ungroup = false)
@test gd2 ≅ groupby(df, :a)

# @rsubset!
# skipmissing, ungroup
@test_throws ArgumentError @rsubset!(copy(df), :b == 3; skipmissing = false)
gd2 = @rsubset!(deepcopy(gd), first(true); ungroup = false)
@test gd2 ≅ groupby(df, :a)

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