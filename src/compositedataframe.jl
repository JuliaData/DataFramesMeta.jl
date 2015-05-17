using Compat

export AbstractCompositeDataFrame, CompositeDataFrame

abstract AbstractCompositeDataFrame <: AbstractDataFrame

function CompositeDataFrame(columns::Vector{Any},
                            cnames::Vector{Symbol} = gennames(length(columns)))
    # TODO: length checks
    typename = symbol("CompositeDF" * string(gensym()))
    e = :(type $(typename) <: AbstractCompositeDataFrame end)
    e.args[3].args = Any[:($(cnames[i]) :: $(typeof(columns[i]))) for i in 1:length(columns)]
    eval(e)   # create the type
    typ = eval(typename)
    return typ(columns...)
end

CompositeDataFrame(; kwargs...) =
    CompositeDataFrame(Any[ v for (k, v) in kwargs ],
                       Symbol[ k for (k, v) in kwargs ])

# CompositeDataFrame(df::DataFrame) = CompositeDataFrame(df.columns, names(df))

CompositeDataFrame(adf::AbstractDataFrame) =
    CompositeDataFrame(values(adf), names(adf))
    
CompositeDataFrame(adf::AbstractDataFrame, nms::Vector{Symbol}) =
    CompositeDataFrame(values(adf), nms)


DataFrames.DataFrame(cdf::AbstractCompositeDataFrame) = DataFrame(values(cdf), names(cdf))


#########################################
## basic stuff
#########################################

Base.names{T <: AbstractCompositeDataFrame}(cdf::T) = @compat fieldnames(T)

DataFrames.ncol(cdf::AbstractCompositeDataFrame) = length(names(cdf))
DataFrames.nrow(cdf::AbstractCompositeDataFrame) = ncol(cdf) > 0 ? length(cdf.(1))::Int : 0

Base.values(cdf::AbstractCompositeDataFrame) = Any[ cdf.(i) for i in 1:length(cdf) ]
                
function Base.hcat(df1::AbstractCompositeDataFrame, df2::AbstractCompositeDataFrame)
    nms = DataFrames.make_unique([names(df1); names(df2)])
    columns = Any[values(df1)..., values(df2)...]
    return CompositeDataFrame(columns, nms)
end
Base.hcat(df1::DataFrame, df2::AbstractCompositeDataFrame) = hcat(df1, DataFrame(df2))
Base.hcat(df1::AbstractCompositeDataFrame, df2::AbstractDataFrame) = hcat(DataFrame(df1), DataFrame(df2))
Base.hcat(df1::AbstractDataFrame, df2::AbstractCompositeDataFrame) = hcat(DataFrame(df1), DataFrame(df2))

DataFrames.index(cdf::AbstractCompositeDataFrame) = DataFrames.Index(names(cdf))

#########################################
## getindex
#########################################

Base.getindex(cdf::AbstractCompositeDataFrame, col_inds::DataFrames.ColumnIndex) = cdf.(col_inds)
Base.getindex{T <: DataFrames.ColumnIndex}(cdf::AbstractCompositeDataFrame, col_inds::AbstractVector{T}) = CompositeDataFrame(Any[ cdf.(col_inds[i]) for i = 1:length(col_inds) ], names(cdf)[col_inds])
Base.getindex(cdf::AbstractCompositeDataFrame, row_inds, col_inds::DataFrames.ColumnIndex) = cdf.(col_inds)[row_inds]
Base.getindex(cdf::AbstractCompositeDataFrame, row_inds, col_inds) = 
    CompositeDataFrame(Any[ cdf.(col_inds[i])[row_inds] for i = 1:length(col_inds) ],
                       Symbol[ names(cdf)[i] for i = 1:length(col_inds) ])

function Base.getindex(cdf::AbstractCompositeDataFrame, row_inds, col_inds::UnitRange)
    if col_inds.start == 1 && col_inds.stop == length(cdf)
        return typeof(cdf)([ cdf.(i)[row_inds] for i in 1:length(cdf) ]...)
    else
        return CompositeDataFrame(Any[ cdf.(col_inds[i])[row_inds] for i = 1:length(col_inds) ], names(cdf)[col_inds])
    end
end

#########################################
## LINQ-like operations
#########################################


order(d::AbstractCompositeDataFrame; args...) =
    d[sortperm(DataFrame(args...)), :]
                       
transform(d::AbstractCompositeDataFrame; kwargs...) =
    CompositeDataFrame(Any[values(d)..., [ isa(v, Function) ? v(d) : v for (k,v) in kwargs ]...],
                       Symbol[names(d)..., [ k for (k,v) in kwargs ]...])

select(d::AbstractCompositeDataFrame; kwargs...) =
    CompositeDataFrame(Any[ v for (k,v) in kwargs ],
                       Symbol[ k for (k,v) in kwargs ])
