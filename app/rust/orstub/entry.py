from ortools.linear_solver import pywraplp

status_table = {
    pywraplp.Solver.OPTIMAL: "optimal",
    pywraplp.Solver.FEASIBLE: "feasible",
    pywraplp.Solver.INFEASIBLE: "infeasible",
    pywraplp.Solver.UNBOUNDED: "unbounded",
    pywraplp.Solver.ABNORMAL: "abnormal",
    pywraplp.Solver.MODEL_INVALID: "model_invalid",
    pywraplp.Solver.NOT_SOLVED: "not_solved",
}


def entry(costs):
    # Data
    num_workers = len(costs)
    num_tasks = len(costs[0])
    # print(
    #     f"w={num_workers}{type(costs)},t={num_tasks}{type(costs[0])}{type(costs[0][0])}"
    # )

    # begin = time.time()

    solver = pywraplp.Solver.CreateSolver("SCIP")
    solver.set_time_limit(30)
    if not solver:
        return

    x = {}
    for i in range(num_workers):
        for j in range(num_tasks):
            x[i, j] = solver.IntVar(0, 1, "")

    # Constraints

    if num_workers < num_tasks:
        for i in range(num_workers):
            solver.Add(solver.Sum([x[i, j] for j in range(num_tasks)]) == 1)
        for j in range(num_tasks):
            solver.Add(solver.Sum([x[i, j] for i in range(num_workers)]) <= 1)
    else:
        for i in range(num_workers):
            solver.Add(solver.Sum([x[i, j] for j in range(num_tasks)]) <= 1)
        for j in range(num_tasks):
            solver.Add(solver.Sum([x[i, j] for i in range(num_workers)]) == 1)

    # Objective
    objective_terms = []
    for i in range(num_workers):
        for j in range(num_tasks):
            objective_terms.append(costs[i][j] * x[i, j])
    solver.Minimize(solver.Sum(objective_terms))

    # print(f"prep={time.time()-begin}")

    # Solve
    # print(f"Solving with {solver.SolverVersion()}")
    # begin = time.time()
    status = solver.Solve()
    # print(f"solve={time.time()-begin}")

    res = []

    # Print solution.
    if not (status == pywraplp.Solver.OPTIMAL or status == pywraplp.Solver.FEASIBLE):
        print(f"No solution found.: {status_table[status]}")
        return None

    # print(f"Total cost = {solver.Objective().Value()}\n")
    for i in range(num_workers):
        for j in range(num_tasks):
            # Test if x[i,j] is 1 (with tolerance for floating point arithmetic).
            if x[i, j].solution_value() > 0.5:
                # res[i] = j
                res.append((i, j))
                # print(f"Worker {i} assigned to task {j}." + f" Cost: {costs[i][j]}")

    return res
