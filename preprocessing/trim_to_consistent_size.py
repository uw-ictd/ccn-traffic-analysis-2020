import pandas as pd
import dask.dataframe
import os

import infra.dask

def trim_flows_recursive(in_path, out_path, client):
    subfiles = sorted(os.listdir(in_path))

    future_handles = []
    for subfile in subfiles:
        df_path = os.path.join(in_path, subfile)
        df_out_path = os.path.join(out_path, subfile)

        df = dask.dataframe.read_parquet(df_path, engine="fastparquet")
        df = df.loc[(df.index >= '2019-03-10 00:00') & (df.index < '2020-05-03 00:00')]

        df = df.astype({"user_port": int,
                        "dest_port": int,
                        "bytes_up": int,
                        "bytes_down": int,
                        "protocol": int,
                        "ambiguous_fqdn_count": int
                        })

        handle = infra.dask.clean_write_parquet(df, df_out_path, compute=False)
        future_handles.append(handle)

    print("Recursive flow trim now")
    client.compute(future_handles, sync=True)


def trim_dns_recursive(in_path, out_path, client):
    subfiles = sorted(os.listdir(in_path))

    future_handles = []
    for subfile in subfiles:
        df_path = os.path.join(in_path, subfile)
        df_out_path = os.path.join(out_path, subfile)

        df = dask.dataframe.read_parquet(df_path, engine="fastparquet")
        df = df.loc[(df.index >= '2019-03-10 00:00') & (df.index < '2020-05-03 00:00')]

        handle = infra.dask.clean_write_parquet(df, df_out_path, compute=False)
        future_handles.append(handle)

    print("Recursive dns trim now")
    client.compute(future_handles, sync=True)


def trim_flows_flat_noindex(in_path, out_path):
    df = dask.dataframe.read_parquet(in_path, engine="fastparquet")
    df = df.loc[(df["start"] >= '2019-03-10 00:00') & (df["start"] < '2020-05-03 00:00')]
    df = df.astype({"user_port": int,
                    "dest_port": int,
                    "bytes_up": int,
                    "bytes_down": int,
                    "protocol": int,
                    })

    print("Single layer flow trim now")
    infra.dask.clean_write_parquet(df, out_path)


def trim_atypical_flows_flat_noindex(in_path, out_path):
    df = dask.dataframe.read_parquet(in_path, engine="fastparquet")
    df = df.loc[(df["start"] >= '2019-03-10 00:00') & (df["start"] < '2020-05-03 00:00')]
    df = df.astype({"a_port": int,
                    "b_port": int,
                    "bytes_a_to_b": int,
                    "bytes_b_to_a": int,
                    "protocol": int,
                    })

    print("Single layer flow trim now")
    infra.dask.clean_write_parquet(df, out_path)


def trim_dns_flat(in_path, out_path):
    df = dask.dataframe.read_parquet(in_path, engine="fastparquet")
    df = df.loc[(df.index >= '2019-03-10 00:00') & (df.index < '2020-05-03 00:00')]

    print("Single layer dns trim now")
    infra.dask.clean_write_parquet(df, out_path)


def trim_transactions_flat_noindex(in_path, out_path):
    df = dask.dataframe.read_parquet(in_path, engine="fastparquet")
    df = df.loc[(df["timestamp"] >= '2019-03-10 00:00') & (df["timestamp"] < '2020-05-03 00:00')]
    print("Single layer transaction trim  now")
    df["amount_bytes"] = df["amount_bytes"].fillna(value=0)
    df = df.astype({"amount_bytes": int, "amount_idr": int})
    infra.dask.clean_write_parquet(df, out_path)


if __name__ == "__main__":
    client = infra.dask.setup_dask_client()
    trim_flows_recursive("data/clean/flows/typical_fqdn_TZ_DIV_user_INDEX_start",
                         "data/clean/flows/typical_fqdn_TM_DIV_user_INDEX_start",
                         client)

    trim_flows_flat_noindex("data/clean/flows/typical_TZ_DIV_none_INDEX_user",
                            "data/clean/flows/typical_TM_DIV_none_INDEX_user")

    trim_atypical_flows_flat_noindex(
        "data/clean/flows/nouser_TZ_DIV_none_INDEX_none",
        "data/clean/flows/nouser_TM_DIV_none_INDEX_none"
    )

    trim_atypical_flows_flat_noindex(
        "data/clean/flows/p2p_TZ_DIV_none_INDEX_none",
        "data/clean/flows/p2p_TM_DIV_none_INDEX_none"
    )

    trim_dns_recursive("data/clean/dns/successful_TZ_DIV_user_INDEX_timestamp",
                       "data/clean/dns/successful_TM_DIV_user_INDEX_timestamp",
                       client)

    trim_dns_flat("data/clean/dns/successful_TZ_DIV_none_INDEX_timestamp",
                  "data/clean/dns/successful_TM_DIV_none_INDEX_timestamp")

    trim_transactions_flat_noindex("data/clean/transactions_TZ",
                                   "data/clean/transactions_TM")

