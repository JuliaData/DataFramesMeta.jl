using DataFramesMeta, Random, BenchmarkTools, Statistics

N = 1_000_000
K = 10

df = DataFrame(
	id = rand([Symbol("id", i) for i=1:K], N),
	v1 = rand(1:5, N),
	v2 = rand(1:5, N),
	v3 = rand(N)
);

gd = groupby(df, :id);

function complicated_vec(x, y, z)
	mx = mean(x)
	sy = std(y)
	vz = var(z)

	temp = (mx * sy) / vz

	a = @. mx * sy + vz + temp

	ma = mean(a)
	sa = std(a)

	@. (a - ma) / sa
end

function complicated_scalar(x, y, z)
	mx = mean(x)
	sy = std(y)
	vz = var(z)

	temp = (mx * sy) / vz

	a = mx * sy + vz + temp

	a * 1000
end

demean(x) = x .- mean(x)

function DataFrames_timings(df, gd)
	df_res = transform(df,
		:v1 => (t -> t .- mean(t)) => :res1,
		:v2 => demean => :res2,
		[:v1, :v2] => (+) => :res3,
		:id => string => :res4,
		[:v1, :v2, :v3] => complicated_vec => :res5,
		[:v1, :v2, :v3] => ((a, b, c) -> @. a + b * c * c + a) => :res6a,
		[:v1, :v2, :v3] =>
		(
			(a, b, c) -> begin
				d = Vector{Float64}(undef, length(a))
				for i in eachindex(d)
					d[i] = a[i] + b[i] * c[i] * c[i] + a[i]
				end
				d
			end
		) => :res6b
	)

	gd_res = combine(gd,
		:v1 => (t -> mean(t)) => :res7,
		:v2 => mean => :res8,
		[:v1, :v2] => ((t, s) -> std(t) + std(s)) => :res9,
		[:v1, :v2, :v3] => complicated_scalar => :res10,
		[:v1, :v2, :v3] => ((a, b, c)  -> first(a) + mean(b) * std(a) + last(c)) => :res11,
	)

	return(df_res, gd_res)
end

function DataFramesMeta_timings(df, gd)
	df_res = @transform(df,
		:res1 = :v1 .- mean(:v1),
		:res2 = demean(:v2),
		:res3 = :v1 + :v2,
		:res4 = string(:id),
		:res5 = complicated_vec(:v1, :v2, :v3),
		:res6a = @.(:v1 + :v2 + :v3 * :v3 + :v1),
		:res6b = begin
			d = Vector{Float64}(undef, length(:v1))
			for i in eachindex(d)
				d[i] = :v1[i] + :v2[i] * :v3[i] * :v3[i] + :v1[i]
			end
			d
		end
	)

	gd_res = @combine(gd,
		:res7 = mean(:v1),
		:res8 = (t -> mean(t))(:v2),
		:res9 = std(:v1) + std(:v2),
		:res10 = complicated_scalar(:v1, :v2, :v3),
		:res11 = first(:v1) + mean(:v2) * std(:v1) + last(:v3)
	)

	return(df_res, gd_res)
end

println("DataFrames benchmark timings")
@btime DataFrames_timings($df, $gd);
println("DataFramesMeta benchmark, timings")
@btime DataFramesMeta_timings($df, $gd);

N = 10
K = 10

df2 = DataFrame(
	:id = rand([Symbol("id", i) for i=1:K], N),
	:v1 = rand(1:5, N),
	:v2 = rand(1:5, N),
	:v3 = rand(N)
);

println("DataFramesMeta raw timing")
@time @select(df2, :res1 = :v1 .- mean(:v1));
@time @select(df2, :res2 = demean(:v2));
@time @select(df2, :res3 = :v1 + :v2);
@time @select(df2, :res4 = string(:id));
@time @select(df2, :res5 = complicated_vec(:v1, :v2, :v3));
@time @select(df2, :res6a = @.(:v1 + :v2 + :v3 * :v3 + :v1));
@time @select(df2, :res6b = begin
	d = Vector{Float64}(undef, length(:v1))
	for i in eachindex(d)
		d[i] = :v1[i] + :v2[i] * :v3[i] * :v3[i] + :v1[i]
	end
	d
end);

nothing