# calculates opr from a set of matches
import polars as pl
import motherduck as md
import numpy as np


def calculate_opr_ccwm_dpr ( matches:pl.DataFrame) -> pl.DataFrame:

    team_list = set(
        matches.select([
            'red_0_id','red_1_id','blue_0_id','blue_1_id'
        ]).unpivot(value_name='id').get_column('id')
    )

    # courtesy: https://github.com/owsorber/FTC_OPR_Calculator/blob/master/OPR.py
    # i'm sure there's a more elegant way to do this!
    # in particular by melting the wide format to a list of matches
    M=[]
    for match in matches.iter_rows(named=True):
        r=[]
        for team in team_list:
            if match['red_0_id'] == team or match['red_1_id'] == team:
                r.append(1)
            else:
                r.append(0)
        M.append(r)

        b = []
        for team in team_list:
            if match['blue_0_id'] == team or match['blue_1_id'] == team:
                b.append(1)
            else:
                b.append(0)
        M.append(b)

    scores= []
    margins= []

    for match in matches.iter_rows(named=True):
        scores.append([match['red_score']])
        scores.append([match['blue_score']])
        margins.append([match['red_score'] - match['blue_score']])
        margins.append([match['blue_score'] - match['red_score']])

    m_m = np.matrix(M)
    s_m = np.matrix(scores)
    c_m = np.matrix(margins)

    pseudo_inverse = np.linalg.pinv(m_m)

    oprs = np.matmul(pseudo_inverse,s_m)
    ccwms = np.matmul(pseudo_inverse,c_m)


    def get_matrix_into_one_list(m):
        return np.reshape(m,(1,-1)).tolist()[0]


    results = pl.DataFrame( {
        'team_id': list(team_list),
        'opr' : get_matrix_into_one_list(oprs),
        'ccwm': get_matrix_into_one_list(ccwms)
    }).with_columns(
        (pl.col('opr') - pl.col('ccwm')).alias('dpr')
    ).sort(['opr'],descending=True)
    return results

if __name__  == '__main__':
    matches = md.get_matches().filter(pl.col('event_id') == 53463)
    r = calculate_opr_ccwm_dpr(matches)
    print(r)