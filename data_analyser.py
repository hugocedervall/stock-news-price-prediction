


def calc_rate_of_return(pred, actual, buy_class=1):
    returns = []
    for i in range(len(pred)):
        pred_class = pred[i]
        actual_change = actual[i]
        
        if pred_class == buy_class:
            returns.append(actual_change)
    return returns