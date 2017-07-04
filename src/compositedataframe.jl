using Compat

export AbstractCompositeDataFrame, AbstractCompositeDataFrameRow,
       CompositeDataFrame, row

"""
    AbstractCompositeDataFrame

An abstract type that is an `AbstractDataFrame`. Each type that inherits from
this is expected to be a type-stable data frame.
"""
@compat abstract type AbstractCompositeDataFrame <: AbstractDataFrame end

@compat abstract type AbstractCompositeDataFrameRow end


"""
    row(cdf::AbstractCompositeDataFrame, i)

Return row `i` of `cdf` as a `CompositeDataFrameRow`. This object has
the same fields as `cdf` where the type of each field is taken from the `eltype`
of the field in `cdf`.

See also `eachrow(cdf)`.

```julia
df = CompositeDataFrame(x = 1:3, y = [2, 1, 6])
dfr = row(df, 3)
dfr.y   # 6
```
"""
row() = nothing

"""
```julia
CompositeDataFrame(columns::Vector{Any}, cnames::Vector{Symbol})
CompositeDataFrame(columns::Vector{Any}, cnames::Vector{Symbol}, typename::Symbol)
CompositeDataFrame(; kwargs...)
CompositeDataFrame(typename::Symbol; kwargs...)
```

A constructor of an `AbstractCompositeDataFrame` that mimics the `DataFrame`
constructor.  This returns a composite type (not immutable) that is an
`AbstractCompositeDataFrame`.

This uses `eval` to create a new type within the module specified by the
inmodule keyword.

### Arguments

* `columns` : contains the contents of the columns
* `cnames` : the names of the columns
* `typename` : the optional name of the type created
* `kwargs` : the key gives the column names, and the value is the column contents
* `inmodule = DataFramesMeta` : a keyword argument to specify what module you
want to define the type in. Consider passing `current_module()` or
`@__MODULE__` depending on your julia version.

### Examples

```julia
df = CompositeDataFrame(Any[1:3, [2, 1, 2]], [:x, :y])
df = CompositeDataFrame(x = 1:3, y = [2, 1, 2])
df = CompositeDataFrame(:MyDF, x = 1:3, y = [2, 1, 2])
```
"""
function CompositeDataFrame(columns::Vector{Any},
                            cnames::Vector{Symbol} = gennames(length(columns)),
                            typename::Symbol = @compat(Symbol("CompositeDF", gensym()));
                            inmodule = DataFramesMeta)
    rowtypename = @compat Symbol(typename, "Row")
    # TODO: length checks
    type_definition = :(type $(typename) <: AbstractCompositeDataFrame end)
    type_definition.args[3].args = Any[:($(cnames[i]) :: $(typeof(columns[i]))) for i in 1:length(columns)]
    ## do the same for the row iterator type:
    column_definition = :(immutable $(rowtypename) <: AbstractCompositeDataFrameRow end)
    column_definition.args[3].args = Any[:($(cnames[i]) :: $(eltype(columns[i]))) for i in 1:length(columns)]
    typeconv = Expr(:call, rowtypename, [Expr(:ref, Expr(:(.), :d, QuoteNode(nm)), :i) for nm in cnames]...)
    row_method = Expr(:function, :( DataFramesMeta.row(d::$typename, i::Integer) ), typeconv)
    row_call = :($typename($columns...))
    eval(inmodule, Expr(:block, type_definition, column_definition, row_method, row_call))
end

CompositeDataFrame(; inmodule = DataFramesMeta, kwargs...) =
    CompositeDataFrame(Any[ v for (k, v) in kwargs ],
                       Symbol[ k for (k, v) in kwargs ],
                       inmodule = inmodule)
CompositeDataFrame(typename::Symbol; inmodule = DataFramesMeta, kwargs...) =
    CompositeDataFrame(Any[ v for (k, v) in kwargs ],
                       Symbol[ k for (k, v) in kwargs ],
                       typename,
                       inmodule = inmodule)

# CompositeDataFrame(df::DataFrame) = CompositeDataFrame(df.columns, names(df))

CompositeDataFrame(adf::AbstractDataFrame) =
    CompositeDataFrame(DataFrames.columns(adf), names(adf))

CompositeDataFrame(adf::AbstractDataFrame, nms::Vector{Symbol}) =
    CompositeDataFrame(DataFrames.columns(adf), nms)


DataFrames.DataFrame(cdf::AbstractCompositeDataFrame) = DataFrame(DataFrames.columns(cdf), names(cdf))


#########################################
## basic stuff
#########################################

Base.names{T <: AbstractCompositeDataFrame}(cdf::T) = @compat fieldnames(T)

DataFrames.ncol(cdf::AbstractCompositeDataFrame) = length(names(cdf))
DataFrames.nrow(cdf::AbstractCompositeDataFrame) = ncol(cdf) > 0 ? length(getfield(cdf, 1))::Int : 0

DataFrames.columns(cdf::AbstractCompositeDataFrame) = Any[ getfield(cdf, i) for i in 1:length(cdf) ]

function Base.hcat(df1::AbstractCompositeDataFrame, df2::AbstractCompositeDataFrame)
    nms = DataFrames.make_unique([names(df1); names(df2)])
    columns = Any[DataFrames.columns(df1)..., DataFrames.columns(df2)...]
    return CompositeDataFrame(columns, nms)
end
Base.hcat(df1::DataFrame, df2::AbstractCompositeDataFrame) = hcat(df1, DataFrame(df2))
Base.hcat(df1::AbstractCompositeDataFrame, df2::AbstractDataFrame) = hcat(DataFrame(df1), DataFrame(df2))
Base.hcat(df1::AbstractDataFrame, df2::AbstractCompositeDataFrame) = hcat(DataFrame(df1), DataFrame(df2))

DataFrames.index(cdf::AbstractCompositeDataFrame) = DataFrames.Index(names(cdf))

#########################################
## getindex
#########################################

Base.getindex(cdf::AbstractCompositeDataFrame, col_inds::DataFrames.ColumnIndex) = getfield(cdf, col_inds)
Base.getindex{T <: DataFrames.ColumnIndex}(cdf::AbstractCompositeDataFrame, col_inds::AbstractVector{T}) = CompositeDataFrame(Any[ getfield(cdf, col_inds[i]) for i = 1:length(col_inds) ], names(cdf)[col_inds])
Base.getindex(cdf::AbstractCompositeDataFrame, row_inds, col_inds::DataFrames.ColumnIndex) = getfield(cdf, col_inds)[row_inds]
Base.getindex(cdf::AbstractCompositeDataFrame, row_inds, col_inds) =
    CompositeDataFrame(Any[ getfield(cdf, col_inds[i])[row_inds] for i = 1:length(col_inds) ],
                       Symbol[ names(cdf)[i] for i = 1:length(col_inds) ])
Base.getindex(cdf::AbstractCompositeDataFrame, row_inds, ::Colon) = typeof(cdf)([getfield(cdf, i)[row_inds] for i in 1:length(cdf)]...)

function Base.getindex(cdf::AbstractCompositeDataFrame, row_inds, col_inds::UnitRange)
    if col_inds.start == 1 && col_inds.stop == length(cdf)
        return typeof(cdf)([ getfield(cdf, i)[row_inds] for i in 1:length(cdf) ]...)
    else
        return CompositeDataFrame(Any[ getfield(cdf, col_inds[i])[row_inds] for i = 1:length(col_inds) ], names(cdf)[col_inds])
    end
end

#########################################
## Row iterator
#########################################

"""
    CDFRowIterator

An iterator over the rows of an `AbstractCompositeDataFrame`. Each row
is an immutable type with the same names as the parent composite data frame.
This iterator is created by calling `eachrow(df)` where `df` is an
`AbstractCompositeDataFrame`.

See also `row(cdf, i)`.
"""
immutable CDFRowIterator{T <: AbstractCompositeDataFrame}
    df::T
    len::Int
end
DataFrames.eachrow(df::AbstractCompositeDataFrame) = CDFRowIterator(df, nrow(df))

Base.start(itr::CDFRowIterator) = 1
Base.done(itr::CDFRowIterator, i::Int) = i > itr.len
Base.next(itr::CDFRowIterator, i::Int) = (row(itr.df, i), i + 1)
Base.size(itr::CDFRowIterator) = (size(itr.df, 1), )
Base.length(itr::CDFRowIterator) = size(itr.df, 1)
Base.getindex(itr::CDFRowIterator, i::Any) = row(itr.df, i)
Base.map(f::Function, dfri::CDFRowIterator) = [f(row) for row in dfri]

#########################################
## LINQ-like operations
#########################################


DataFrames.order(d::AbstractCompositeDataFrame; args...) =
    d[sortperm(DataFrame(args...)), :]

transform(d::AbstractCompositeDataFrame; kwargs...) =
    CompositeDataFrame(Any[DataFrames.columns(d)..., [ isa(v, Function) ? v(d) : v for (k,v) in kwargs ]...],
                       Symbol[names(d)..., [ k for (k,v) in kwargs ]...])

Base.select(d::AbstractCompositeDataFrame; kwargs...) =
    CompositeDataFrame(Any[ v for (k,v) in kwargs ],
                       Symbol[ k for (k,v) in kwargs ])
