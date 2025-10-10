def dict_cal(dicta):
    dict2={}
    for item in dicta:
        cat=item["category"]
        Sum_total=item["price"]*item["quantity"]
        if cat in dict2:
            dict2[cat]+=Sum_total
        else:
            dict2[cat]=Sum_total
            
    return dict2