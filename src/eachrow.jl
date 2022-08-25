##############################################################################
##
## @eachrow
##
##############################################################################


# Recursive function that traverses the syntax tree of e, replaces instances of
# ":(:(x))" with ":x[row]".
eachrow_replace(x) = x
eachrow_replace(e::QuoteNode) = Expr(:ref, e, :row)

function eachrow_replace(e::Expr)
    if onearg(e, :^)
        return e.args[2]
    end

    # Traverse the syntax tree of e
    col = get_column_expr(e)
    if col !== nothing
        return :($e[row])
    # equivalent to protect_replace_syms
    elseif e.head == :.
        x_new = eachrow_replace(e.args[1])
        y = e.args[2]
        y_new = y isa Expr ? eachrow_replace(y) : y

        return Expr(:., x_new, y_new)
    else
        mapexpr(eachrow_replace, e)
    end
end

protect_eachrow_replace(e) = e
protect_eachrow_replace(e::Expr) = eachrow_replace(e)

function eachrow_replace_dotted(e, membernames)
    x_new = eachrow_repalce(e.args[1])
    y_new = protect_eachrow_repalce(e.args[2])
    Expr(:., x_new, y_new)
end

function eachrow_find_newcols(e::Expr, newcol_decl)
    if e.head == :macrocall && e.args[1] == Symbol("@newcol")
        ea = e.args[3]
        # expression to assign a new column to df
        return (nothing, Any[Expr(:(=), ea.args[1], Expr(:call, ea.args[2], :undef, :_N))])
    else
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
    @eachrow(df, body)

Act on each row of a data frame, producing a new dataframe.
Similar to

```
for row in eachrow(copy(df))
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
transformations. `_N` is introduced to represent the number of rows in the data frame,
`_DF` represents the `DataFrame` including added columns, and `row` represents
the index of the current row.

Changes to the rows do not affect `df` but instead a freshly allocated data frame is returned
by `@eachrow`. Also note that the returned data frame does not share columns
with `df`. See [`@eachrow!`](@ref) which employs the same syntax but modifies
the data frame in-place.

Like with `@transform`, `@eachrow` supports the use of `$DOLLAR` to work with column names
stored as variables. Using `$DOLLAR` with a multi-column selector, such as a `Vector` of
`Symbol`s, is currently unsupported.

`@eachrow` is a thin wrapper around a `for`-loop. As a consequence, inside an `@eachrow`
block, the reserved-word arguments `break` and `continue` function the same as if written
in a `for` loop. Rows unaffected by `break` and `continue` are unmodified, but are still
present in the returned data frame. Also because `@eachrow` is a `for`-loop, re-assigning
global variables inside an `@eachrow` block is discouraged.

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
           @newcol :colX::Vector{Float64}
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
           @newcol :colX::Vector{Float64}
           :colX = $(DOLLAR)varB == 2 ? pi * $(DOLLAR)varA : $(DOLLAR)varB
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
           @newcol :m::Vector{Float64}
           :m = mean(_DF[:, row])
       end
3×3 DataFrame
 Row │ A      B      m
     │ Int64  Int64  Float64
─────┼───────────────────────
   1 │     1      2  2.0
   2 │     2      1  1.66667
   3 │     3      2  1.22222

julia> @eachrow df begin
           :A == 2 && continue
           println(:A)
       end;
1
3

```
"""
macro eachrow(df, body)
    esc(eachrow_helper(df, body, false))
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

Act on each row of a data frame in-place, similar to

```
for row in eachrow(df)
    ... # Actions that modify `df`.
end
```

Includes support for control flow and `begin end` blocks. Since the
"environment" induced by `@eachrow! df` is implicitly a single row of `df`,
use regular operators and comparisons instead of their elementwise counterparts
as in `@with`. Note that the scope within `@eachrow!` is a hard scope.

`eachrow!` also supports special syntax for allocating new columns. The syntax
`@newcol x::Vector{Int}` allocates a new uninitialized column `:x` with an `Vector` container
with eltype `Int`.This feature makes it easier to use `eachrow` for data
transformations. `_N` is introduced to represent the number of rows in the data frame,
`_DF` represents the `dataframe` including added columns, and `row` represents
the index of the current row.

Changes to the rows directly affect `df`. The operation will modify the
data frame in place. See [`@eachrow`](@ref) which employs the same syntax but allocates
a fresh data frame.

Like with `@transform!`, `@eachrow!` supports the use of `$DOLLAR` to work with column names
stored as variables. Using `$DOLLAR` with a multi-column selector, such as a `Vector` of
`Symbol`s, is currently unsupported.

`@eachrow!` is a thin wrapper around a `for`-loop. As a consequence, inside an `@eachrow!`
block, the reserved-word arguments `break` and `continue` function the same as if written
in a `for` loop. Rows unaffected by `break` and `continue` are unmodified, but are still
present in modified. Also because `@eachrow!` is a `for`-loop, re-assigning
global variables inside an `@eachrow` block is discouraged.

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
           @newcol :colX::Vector{Float64}
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
           @newcol :colX::Vector{Float64}
           :colX = $(DOLLAR)varB == 2 ? pi * $(DOLLAR)varA : $(DOLLAR)varB
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
           @newcol :m::Vector{Float64}
           :m = mean(_DF[:, row])
       end
3×3 DataFrame
 Row │ A      B      m
     │ Int64  Int64  Float64
─────┼───────────────────────
   1 │     1      2  2.0
   2 │     2      1  1.66667
   3 │     3      2  1.22222

julia> @eachrow! df begin
           :A == 2 && continue
           println(:A)
       end;
1
3
```
"""
macro eachrow!(df, body)
    esc(eachrow!_helper(df, body))
end
