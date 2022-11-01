function conditionally_add_symbols!(inputs_to_function::AbstractDict,
                                    lhs_assignments::OrderedCollections.OrderedDict, col)
    # if it's already been assigned at top-level,
    # don't add it to the inputs
    if haskey(lhs_assignments, col)
        return lhs_assignments[col]
    else
        return addkey!(inputs_to_function, col)
    end
end

replace_syms_astable!(inputs_to_function::AbstractDict,
                      lhs_assignments::OrderedCollections.OrderedDict, x) = x
replace_syms_astable!(inputs_to_function::AbstractDict,
                      lhs_assignments::OrderedCollections.OrderedDict, q::QuoteNode) =
    conditionally_add_symbols!(inputs_to_function, lhs_assignments, q)

function replace_syms_astable!(inputs_to_function::AbstractDict,
                               lhs_assignments::OrderedCollections.OrderedDict, e::Expr)
    if onearg(e, :^)
        return e.args[2]
    end

    col = get_column_expr(e)
    if col !== nothing
        return conditionally_add_symbols!(inputs_to_function, lhs_assignments, col)
    elseif e.head == :.
        return replace_dotted_astable!(inputs_to_function, lhs_assignments, e)
    else
        return mapexpr(x -> replace_syms_astable!(inputs_to_function, lhs_assignments, x), e)
    end
end

protect_replace_syms_astable!(inputs_to_function::AbstractDict,
                              lhs_assignments::OrderedCollections.OrderedDict, e) = e
protect_replace_syms_astable!(inputs_to_function::AbstractDict,
                              lhs_assignments::OrderedCollections.OrderedDict, e::Expr) =
    replace_syms!(inputs_to_function, lhs_assignments, e)

function replace_dotted_astable!(inputs_to_function::AbstractDict,
                                 lhs_assignments::OrderedCollections.OrderedDict, e)
    x_new = replace_syms_astable!(inputs_to_function, lhs_assignments, e.args[1])
    y_new = protect_replace_syms_astable!(inputs_to_function, lhs_assignments, e.args[2])
    Expr(:., x_new, y_new)
end

is_column_assigment(ex) = false
function is_column_assigment(ex::Expr)
    ex.head == :(=) && (get_column_expr(ex.args[1]) !== nothing)
end

is_multi_column_assignment(ex) = false
function is_multi_column_assignment(ex::Expr)
    if ex.head == :(=)
        exarg = ex.args[1]
        if exarg isa Expr && exarg.head == :tuple
            return all(!isnothing, get_column_expr.(exarg.args))
        else
            return false
        end
    else
        return false
    end
end

# Taken from MacroTools.jl
# No docstring so assumed unstable
block(ex) = isexpr(ex, :block) ? ex : :($ex;)

sym_or_str_to_sym(x::Union{AbstractString, Symbol}) = Symbol(x)
sym_or_str_to_sym(x) =
    throw(ArgumentError("New columns created inside @astable must be Symbols or AbstractStrings"))

function get_source_fun_astable(ex; exprflags = deepcopy(DEFAULT_FLAGS))
    inputs_to_function = Dict{Any, Symbol}()
    lhs_assignments = OrderedCollections.OrderedDict{Any, Symbol}()

    # Make sure all top-level assignments are
    # in the args vector
    ex = block(MacroTools.flatten(ex))
    exprs = map(ex.args) do arg
        if is_column_assigment(arg)
            lhs = get_column_expr(arg.args[1])
            rhs = arg.args[2]
            new_ex = replace_syms_astable!(inputs_to_function, lhs_assignments, rhs)
            if haskey(inputs_to_function, lhs)
                new_lhs = inputs_to_function[lhs]
                lhs_assignments[lhs] = new_lhs
            else
                new_lhs = addkey!(lhs_assignments, lhs)
            end

            Expr(:(=), new_lhs, new_ex)
        elseif is_multi_column_assignment(arg)
            @show exarg = arg.args[1]
            lhss = get_column_expr.(exarg.args)
            rhs = arg.args[2]
            new_ex = replace_syms_astable!(inputs_to_function, lhs_assignments, rhs)
            new_lhss = Expr(:tuple,)
            for lhs in lhss
                @show lhs
                if haskey(inputs_to_function, lhs)
                    new_lhs = inputs_to_function[lhs]
                    lhs_assignments[lhs] = new_lhs
                else
                    new_lhs = addkey!(lhs_assignments, lhs)
                end
                push!(new_lhss.args, new_lhs)
            end
            @show new_lhs
            @show new_ex
            @show new_lhss
            t = Expr(:(=), new_lhss, new_ex)
            @show MacroTools.prettify(t)
            t
        else
            replace_syms_astable!(inputs_to_function, lhs_assignments, arg)
        end
    end
    source = :(DataFramesMeta.make_source_concrete($(Expr(:vect, keys(inputs_to_function)...))))

    inputargs = Expr(:tuple, values(inputs_to_function)...)
    nt_iterator = (:(DataFramesMeta.sym_or_str_to_sym($k) => $v) for (k, v) in lhs_assignments)
    nt_expr = Expr(:tuple, Expr(:parameters, nt_iterator...))

    body = Expr(:block, Expr(:block, exprs...), nt_expr)

    fun = quote
        $inputargs -> begin
            $body
        end
    end

    # TODO: Add passmissing support by
    # checking if any input arguments missing,
    # and if-so, making a named tuple with
    # missing values
    if exprflags[BYROW_SYM][]
        fun = :(ByRow($fun))
    end

    return source, fun
end