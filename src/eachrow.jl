##############################################################################
##
## @eachrow
##
##############################################################################

# Recursive function that traverses the syntax tree of e, replaces instances of
# ":(:(x))" with ":x[row]".
function eachrow_replace(e::Expr)
    # Traverse the syntax tree of e
    if onearg(e, :cols)
        # cols(:x) becomes cols(:x)[row]
        return Expr(:ref, Expr(:call, :cols, e.args[2]), :row)
    end

    if e.head == :.
        if e.args[1] isa QuoteNode
            e.args[1] = Expr(:ref, e.args[1], :row)
            return e
        else
            return e
        end
    end

    Expr(e.head, (isempty(e.args) ? e.args : map(eachrow_replace, e.args))...)
end

eachrow_replace(e::QuoteNode) = Expr(:ref, e, :row)

# Set the base case for helper, i.e. for when expand hits an object of type
# other than Expr (generally a Symbol or a literal).
eachrow_replace(x) = x

function eachrow_find_newcols(e::Expr, newcol_decl)
    if e.head == :macrocall && e.args[1] == Symbol("@newcol")
        ea = e.args[3]
        # expression to assign a new column to df
        return (nothing, Any[Expr(:(=), ea.args[1], Expr(:call, ea.args[2], :undef, :_N))])
    else
        if isempty(e.args)
            return (e.args, Any[])
        end
        newargs = Any[]
        for ea in e.args
            (nea, newcol) = eachrow_find_newcols(ea, newcol_decl)
            nea != nothing && push!(newargs, nea)
            nea == nothing && length(newcol) > 0 && append!(newcol_decl, newcol)
        end
        return (Expr(e.head, newargs...), newcol_decl)
    end
end

eachrow_find_newcols(x, newcol_decl) = (x, Any[])

function eachrow_helper(df, body, deprecation_warning)
    # @deprecate cannot be used because eachrow is a macro, and the @warn should not be in
    # eachrow itself because then it will be displayed when the macro is evaluated.
    deprecation_warning && @warn "`@byrow!` and `@byrow` are deprecated, use `@eachrow` instead."
    e_body, e_newcols = eachrow_find_newcols(body, Any[])
    _df = gensym()
    quote
        let $_df = $df
            local _N = nrow($_df)
            local _DF = @transform($_df, $(e_newcols...))
            $(with_helper(:_DF, :(for row = 1:_N
                $(eachrow_replace(e_body))
            end)))
            _DF
        end
    end
end

"""
    @byrow!(d, expr)

Deprecated version of `@eachrow`, see: [`@eachrow`](@ref)

Acts the exact same way. It does not change the input argument `d` in-place.
"""
macro byrow!(df, body)
    esc(eachrow_helper(df, body, true))
end

"""
    @byrow(d, expr)

Deprecated version of `@eachrow`, see: [`@eachrow`](@ref)

Acts the exact same way.
"""
macro byrow(d, body)
    esc(eachrow_helper(d, body, true))
end

"""
    @eachrow(df, body)

Act on each row of a data frame, producing a new dataframe.
Similar to

```
for row in eachrow(df)
    ...
end
```

Includes support for control flow and `begin end` blocks. Since the
"environment" induced by `@eachrow df` is implicitly a single row of `df`,
use regular operators and comparisons instead of their elementwise counterparts
as in `@with`. Note that the scope within `@eachrow` is a hard scope.

`eachrow` also supports special syntax for allocating new columns. The syntax
`@newcol x::Vector{Int}` allocates a new uninitialized column `:x` with an `Vector` container
with eltype `Int`.This feature makes it easier to use `eachrow` for data
transformations. `_N` is introduced to represent the number of rows in the dataframe,
`_DF` represents the `DataFrame` including added columns, and `row` represents
the index of the current row.

Changes to the rows do not affect `df` but instead a freshly allocated data frame is returned
by `@eachrow`. Also note that the returned data frame does not share columns
with `df`.

Like with `@transform`, `@eachrow` supports the use of `cols` to work with column names
stored as variables. Using `cols` with a multi-column selector, such as a `Vector` of
`Symbol`s, is currently unsupported.

### Arguments

* `df` : an `AbstractDataFrame`
* `expr` : expression operated on row by row

### Returns

The modified `AbstractDataFrame`.

### Examples

```julia
julia> using DataFramesMeta

julia> df = DataFrame(A = 1:3, B = [2, 1, 2]);

julia> let x = 0
            @eachrow df begin
                if :A + :B == 3
                    x += 1
                end
            end  #  This doesn't work without the let
            x
        end
2

julia> @eachrow df begin
            if :A > :B
                :A = 0
            end
        end
3×2 DataFrame
 Row │ A      B
     │ Int64  Int64
─────┼──────────────
   1 │     1      2
   2 │     0      1
   3 │     0      2

julia> df2 = @eachrow df begin
           @newcol colX::Vector{Float64}
           :colX = :B == 2 ? pi * :A : :B
       end
3×3 DataFrame
 Row │ A      B      colX
     │ Int64  Int64  Float64
─────┼───────────────────────
   1 │     1      2  3.14159
   2 │     2      1  1.0
   3 │     3      2  9.42478

julia> varA = :A; varB = :B;

julia> df2 = @eachrow df begin
           @newcol colX::Vector{Float64}
           :colX = cols(varB) == 2 ? pi * cols(varA) : cols(varB)
       end
3×3 DataFrame
 Row │ A      B      colX
     │ Int64  Int64  Float64
─────┼───────────────────────
   1 │     1      2  3.14159
   2 │     2      1  1.0
   3 │     3      2  9.42478

julia> x = [1, 1, 1];

julia> @eachrow df begin
           x[row] = :A
       end;

julia> x
3-element Vector{Int64}:
 1
 2
 3

julia> @eachrow df begin
           @newcol m::Vector{Float64}
           :m = mean(_DF[:, row])
       end
3×3 DataFrame
 Row │ A      B      m
     │ Int64  Int64  Float64
─────┼───────────────────────
   1 │     1      2  2.0
   2 │     2      1  1.66667
   3 │     3      2  1.22222

```
"""
macro eachrow(d, body)
    esc(eachrow_helper(d, body, false))
end

function eachrow!_helper(df, body)
    e_body, e_newcols = eachrow_find_newcols(body, Any[])
    _df = gensym()
    quote
        let $_df = $df
            local _N = nrow($_df)
            local _DF = @transform!($_df, $(e_newcols...))
            $(with_helper(:_DF, :(for row = 1:_N
                $(eachrow_replace(e_body))
            end)))
            _DF
        end
    end
end

"""
    @eachrow!(df, body)

Act on each row of a data frame, similar to

```
for row in eachrow(df)
    ... # Actions that modify `df`.
end
```

Includes support for control flow and `begin end` blocks. Since the
"environment" induced by `@eachrow df` is implicitly a single row of `df`,
use regular operators and comparisons instead of their elementwise counterparts
as in `@with`. Note that the scope within `@eachrow!` is a hard scope.

`eachrow!` also supports special syntax for allocating new columns. The syntax
`@newcol x::Vector{Int}` allocates a new uninitialized column `:x` with an `Vector` container
with eltype `Int`.This feature makes it easier to use `eachrow` for data
transformations. `_N` is introduced to represent the number of rows in the dataframe,
`_DF` represents the `dataframe` including added columns, and `row` represents
the index of the current row.

Changes to the rows directly affect `df`. The operation will modify the
data frame in place.

Like with `@transform!`, `@eachrow!` supports the use of `cols` to work with column names
stored as variables. Using `cols` with a multi-column selector, such as a `Vector` of
`Symbol`s, is currently unsupported.

### Arguments

* `df` : an `AbstractDataFrame`
* `expr` : expression operated on row by row

### Returns

The modified `AbstractDataFrame`.

### Examples

```julia
julia> using DataFramesMeta

julia> df = DataFrame(A = 1:3, B = [2, 1, 2]);

julia> let x = 0
            @eachrow! df begin
                if :A + :B == 3
                    x += 1
                end
            end  #  This doesn't work without the let
            x
        end
2

julia> df2 = copy(df);

julia> @eachrow! df2 begin
           if :A > :B
               :A = 0
           end
       end;

julia> df2
3×2 DataFrame
 Row │ A      B
     │ Int64  Int64
─────┼──────────────
   1 │     1      2
   2 │     0      1
   3 │     0      2

julia> df2 = copy(df);

julia> @eachrow! df2 begin
           @newcol colX::Vector{Float64}
           :colX = :B == 2 ? pi * :A : :B
       end
3×3 DataFrame
 Row │ A      B      colX
     │ Int64  Int64  Float64
─────┼───────────────────────
   1 │     1      2  3.14159
   2 │     2      1  1.0
   3 │     3      2  9.42478

julia> varA = :A; varB = :B;

julia> df2 = copy(df);

julia> @eachrow! df2 begin
           @newcol colX::Vector{Float64}
           :colX = cols(varB) == 2 ? pi * cols(varA) : cols(varB)
       end
3×3 DataFrame
 Row │ A      B      colX
     │ Int64  Int64  Float64
─────┼───────────────────────
   1 │     1      2  3.14159
   2 │     2      1  1.0
   3 │     3      2  9.42478

julia> x = [1, 1, 1];

julia> @eachrow! df begin
           x[row] = :A
       end;

julia> x
3-element Vector{Int64}:
 1
 2
 3

julia> @eachrow! df begin
           @newcol m::Vector{Float64}
           :m = mean(_DF[:, row])
       end
3×3 DataFrame
 Row │ A      B      m
     │ Int64  Int64  Float64
─────┼───────────────────────
   1 │     1      2  2.0
   2 │     2      1  1.66667
   3 │     3      2  1.22222
```
"""
macro eachrow!(d, body)
    esc(eachrow!_helper(d, body))
end
