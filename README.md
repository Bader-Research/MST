# MST: Parallel Minimum Spanning Forest

Minimum Spanning Tree (MST) is one of the most studied combinatorial
problems with practical applications in VLSI layout, wireless
communication, and distributed networks, recent problems in biology
and medicine such as cancer detection, medical imaging, and
proteomics, and national security and bioterrorism such as detecting
the spread of toxins through populations in the case of
biological/chemical warfare. Most of the previous attempts for
improving the speed of MST using parallel computing are too
complicated to implement or perform well only on special graphs with
regular structure.

This parallel code (a module for SIMPLE/SMP) provides implementations
of four parallel MST algorithms (three variations of Boruvka plus our
new approach) for arbitrary sparse graphs that for the first time give
speedup when compared with the best sequential algorithm. In fact, our
algorithms also solve the minimum spanning forest problem. Our new
implementation achieves good speedups over a wide range of input
graphs with regular and irregular structures, including the graphs
used by previous parallel MST studies.

References:

D.A. Bader and G. Cong, "Fast Shared-Memory Algorithms for Computing
the Minimum Spanning Forest of Sparse Graphs," 18th IEEE Int'l
Parallel and Distributed Processing Symp. (IPDPS), Santa Fe, NM, April
26-30, 2004.

