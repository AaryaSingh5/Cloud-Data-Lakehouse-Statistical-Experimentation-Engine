# Experiment: checkout_test

Planted lift: 10.0%. The same planted lift is applied to every data source.

| source | units | revenue lift | 95% CI | p (Holm) | conversion difference | p (Holm) | revenue significant | bias vs planted |
|---|---|---|---|---|---|---|---|---|
| truth | 1957 | +10.6% | [+4.7%, +16.5%] | 0.001706 | 0 | 1 | yes | +0.6% |
| platform | 1957 | +9.6% | [-13.6%, +32.9%] | 0.4505 | 0.03016 | 0.4505 | no | -0.4% |
| baseline | 1957 | -35.1% | [-68.6%, -1.7%] | 0.1583 | -0.002047 | 1 | no | -45.1% |

Read each row against the truth row. A source whose lift, interval, or significance differs from truth has had its conclusion changed by its data.

