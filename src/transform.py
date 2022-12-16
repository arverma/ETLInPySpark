__author__ = "aman.rv"


def transform(df_dict):
    fact = df_dict["fact"]
    lookup = df_dict["lookup"]
    return fact.join(lookup, "pincode")
