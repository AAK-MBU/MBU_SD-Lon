"""Functions that defines errors to be handled by the robot"""
import pandas as pd

from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection

from robot_framework.subprocesses.helper_functions import get_items_from_query

from robot_framework.worker_data.KV2_data import tillaeg_pairs


def kv1(overenskomst: int, orchestrator_connection: OrchestratorConnection):
    """
    CASE: ANSAT PÅ OVERENSKOMST 47302 OG INSTITUTIONSKODE IKKE XC

    Arguments:
        overenskomst (int): The "overenskomst" to look for
        connection_string (string): Connection string for pyodbc connection

    Returns:
        items (list | None): List of items from the SELECT query. If no elements fits the query then returns None
    """

    sql = f"""
        SELECT
            ans.Tjenestenummer, ans.Overenskomst, ans.Afdeling, ans.Institutionskode, perstam.Navn, ans.Startdato, ans.Slutdato, ans.Statuskode
        FROM [Personale].[sd_magistrat].[Ansættelse_mbu] ans
            right join [Personale].[sd].[personStam] perstam
                on ans.CPR = perstam.CPR
        WHERE
            Slutdato > getdate() and Startdato <= getdate()
            and Overenskomst={overenskomst}
            and Statuskode in ('1', '3', '5')
            and Institutionskode!='XC'
    """

    connection_string = orchestrator_connection.get_constant("FaellesDbConnectionString").value
    items = get_items_from_query(connection_string, sql)

    return items


def kv2(tillaegsnr_par: list, orchestrator_connection: OrchestratorConnection):
    """
    CASE: HAS ONLY ONE OF A PAIR OF 'TILLÆGSNUMRE'

    Arguments:
        tillaegsnr_par (list): List of dicts with keys:
            - 'ovk' (str)
            - 'pair' (tuple of two elements)
            - 'pair_names' (tuple of two elements)
        connection_string (string): Connection string for pyodbc connection

    Returns:
        items (list | None): List of items from the SELECT query. If no elements fits the query then returns None
    """

    items = None

    connection_string = orchestrator_connection.get_constant("FaellesDbConnectionString").value

    for pair in tillaegsnr_par:

        sql = f"""
        SELECT
            ans.Tjenestenummer, til.Tillægsnummer, til.Tillægsnavn, ans.Overenskomst, ans.Afdeling, perstam.Navn, ans.Institutionskode
        FROM
            [Personale].[sd_magistrat].Ansættelse_mbu ans
            right join [Personale].[sd_magistrat].[tillæg_mbu] til
                on ans.AnsættelsesID = til.AnsættelsesID
            right join [Personale].[sd].[personStam] perstam
                on ans.CPR = perstam.CPR
        WHERE
            til.Tillægsnummer in {pair['pair']}
            and ans.Overenskomst = {pair['ovk']}
            and ans.Slutdato > GETDATE() and ans.Startdato < GETDATE()
            and ans.Statuskode in ('1', '3', '5')
            and ans.AnsættelsesID in (
                SELECT
                    ans.AnsættelsesID
                FROM
                    [Personale].[sd_magistrat].Ansættelse_mbu ans
                    right join [Personale].[sd_magistrat].[tillæg_mbu] til
                        on ans.AnsættelsesID = til.AnsættelsesID
                WHERE
                    (til.Tillægsnummer in {pair['pair']} and ans.Overenskomst = {pair['ovk']})
                    and ans.Slutdato > GETDATE()
                    and ans.Startdato < GETDATE()
                    and ans.Statuskode in ('1', '3', '5')
                GROUP BY
                    ans.AnsættelsesID
                HAVING
                    count(distinct til.Tillægsnummer) != 2
            )
        """

        pair_items = get_items_from_query(connection_string, sql)
        if not items:
            items = pair_items
        else:
            items.extend(pair_items if pair_items else [])

    # Combine SD departments with LIS unit names (enhedsnavne)
    items_df = pd.DataFrame(items)

    connection_string_mbu = orchestrator_connection.get_constant("DbConnectionString").value
    lis_dep = lis_enheder(connection_string=connection_string_mbu)
    lis_df = pd.DataFrame(lis_dep).rename(columns={'losid': 'LOSID'})
    lis_df = lis_df[~lis_df['LOSID'].isna()].copy(deep=True)
    lis_df['LOSID'] = lis_df['LOSID'].astype(int, errors='ignore')

    sd_dep = sd_enheder(connection_string=connection_string)
    sd_df = pd.DataFrame(sd_dep)
    sd_df = sd_df[~sd_df['LOSID'].isna()].copy(deep=True)
    sd_df['LOSID'] = sd_df['LOSID'].astype(int, errors='ignore')

    dep_df = pd.merge(left=lis_df[~lis_df['LOSID'].isna()], right=sd_df, how='inner', on='LOSID').rename(columns={'SDafdID': 'Afdeling', 'enhnavn': 'Enhedsnavn'})

    items_dep = pd.merge(left=items_df, right=dep_df, how='left', on='Afdeling')[
        ["Tjenestenummer", "Tillægsnummer", "Tillægsnavn", "Overenskomst", "Afdeling", "Enhedsnavn", "Navn", "Institutionskode"]
    ]

    items = list(items_dep.T.to_dict().values())

    return items


def kv3(accept_ovk_dag: tuple, accept_ovk_skole: tuple, orchestrator_connection: OrchestratorConnection):
    """Ansættelser with wrong overenskomst based on departmentype"""
    connection_string_mbu = orchestrator_connection.get_constant("DbConnectionString").value
    connection_string_faelles = orchestrator_connection.get_constant("FaellesDbConnectionString").value

    # Load department types from LIS stamdata
    lis_stamdata = lis_enheder(connection_string=connection_string_mbu, afdtype=(2, 3, 4, 5, 11, 13))
    losid_tuple = tuple([i['losid'] for i in lis_stamdata])

    # Load corresponding SD department codes
    sd_departments = sd_enheder(losid_tuple=losid_tuple, connection_string=connection_string_faelles)

    # Combine SD and LIS data
    lis_stamdata_df = pd.DataFrame(lis_stamdata).rename(columns={'losid': 'LOSID'})
    lis_stamdata_df['LOSID'] = lis_stamdata_df['LOSID'].astype(int)
    sd_departments_df = pd.DataFrame(sd_departments)
    sd_departments_df['LOSID'] = sd_departments_df['LOSID'].astype(int)

    combined_df = pd.merge(left=lis_stamdata_df, right=sd_departments_df, how='outer', on='LOSID')

    # Filter dagtilbud and skole respectively
    dagtilbud_df = combined_df[(
        (combined_df['afdtype'].isin([2, 3, 4, 5, 11]))
        & ~(combined_df['SDafdID'].isna())
    )]
    dagtilbud_afd = tuple(dagtilbud_df['SDafdID'].values)

    skole_df = combined_df[(
        (combined_df['afdtype'].isin([13]))
        & ~(combined_df['SDafdID'].isna())
    )]
    skole_afd = tuple(skole_df['SDafdID'].values)

    # Collect ansættelser with wrong overenskomst
    items = kv3_1(connection_str=connection_string_faelles, skole_afd=skole_afd, dagtilbud_afd=dagtilbud_afd, accept_ovk_skole=accept_ovk_skole, accept_ovk_dag=accept_ovk_dag)
    items_df = pd.DataFrame(items)

    # # Get AF emails (probably just send to lønservice)
    # af_email = af_losid(connection_str=connection_string_mbu)
    # af_email_df = pd.DataFrame(af_email)
    # combined_df = pd.merge(left=combined_df, right=af_email_df, on="LOSID")

    # Combine with other information
    combined_df = pd.merge(left=combined_df, right=items_df, left_on='SDafdID', right_on='Afdeling')
    combined_df["Startdato"] = combined_df["Startdato"].astype(str)
    combined_df["Slutdato"] = combined_df["Slutdato"].astype(str)
    combined_df = combined_df.rename(columns={"enhnavn": "Enhedsnavn"})[
        ["Tjenestenummer", "Afdeling", "Institutionskode", "Overenskomst", "Enhedsnavn", "Navn", "afdtype_txt"]
    ]

    # Format data as list of dicts. Each list element is a row in the dataframe
    items = list(combined_df.T.to_dict().values())

    return items


def kv3_dev(accept_ovk_dag: tuple, accept_ovk_skole: tuple, orchestrator_connection: OrchestratorConnection):
    """Ansættelser with wrong overenskomst based on departmentype"""
    connection_string_mbu = orchestrator_connection.get_constant("DbConnectionString").value
    connection_string_faelles = orchestrator_connection.get_constant("FaellesDbConnectionString").value

    # Load department types from LIS stamdata
    lis_stamdata = lis_enheder(connection_string=connection_string_mbu, afdtype=(2, 3, 4, 5, 11, 13))
    losid_tuple = tuple([i['losid'] for i in lis_stamdata])

    # Load corresponding SD department codes
    sd_departments = sd_enheder(losid_tuple=losid_tuple, connection_string=connection_string_faelles)

    # Combine SD and LIS data
    lis_stamdata_df = pd.DataFrame(lis_stamdata).rename(columns={'losid': 'LOSID'})
    lis_stamdata_df['LOSID'] = lis_stamdata_df['LOSID'].astype(int)
    sd_departments_df = pd.DataFrame(sd_departments)
    sd_departments_df['LOSID'] = sd_departments_df['LOSID'].astype(int)

    combined_df = pd.merge(left=lis_stamdata_df, right=sd_departments_df, how='outer', on='LOSID')

    # Filter dagtilbud and skole respectively
    dagtilbud_df = combined_df[(
        (combined_df['afdtype'].isin([2, 3, 4, 5, 11]))
        & ~(combined_df['SDafdID'].isna())
    )]
    dagtilbud_afd = tuple(dagtilbud_df['SDafdID'].values)

    skole_df = combined_df[(
        (combined_df['afdtype'].isin([13]))
        & ~(combined_df['SDafdID'].isna())
    )]
    skole_afd = tuple(skole_df['SDafdID'].values)

    # Collect ansættelser with wrong overenskomst
    items = kv3_1_dev(connection_str=connection_string_faelles, skole_afd=skole_afd, dagtilbud_afd=dagtilbud_afd, accept_ovk_skole=accept_ovk_skole, accept_ovk_dag=accept_ovk_dag)
    items_df = pd.DataFrame(items)

    # # Get AF emails (probably just send to lønservice)
    # af_email = af_losid(connection_str=connection_string_mbu)
    # af_email_df = pd.DataFrame(af_email)
    # combined_df = pd.merge(left=combined_df, right=af_email_df, on="LOSID")

    # Combine with other information
    combined_df = pd.merge(left=combined_df, right=items_df, left_on='SDafdID', right_on='Afdeling')
    combined_df["Startdato"] = combined_df["Startdato"].astype(str)
    combined_df["Slutdato"] = combined_df["Slutdato"].astype(str)
    combined_df = combined_df.rename(columns={"enhnavn": "Enhedsnavn"})[
        ["Tjenestenummer", "Afdeling", "Institutionskode", "Overenskomst", "Enhedsnavn", "Navn", "afdtype_txt"]
    ]

    # Format data as list of dicts. Each list element is a row in the dataframe
    items = list(combined_df.T.to_dict().values())

    return items


def lis_enheder(connection_string: str, afdtype: tuple | None = None):
    """Get the right departments from LIS stamdata"""
    sql = """
        SELECT
            distinct lisid, losid, enhnavn, afdtype, afdtype_txt
        FROM
            [BuMasterdata].[dbo].[VIEW_MD_STAMDATA_AKTUEL]
    """
    sql += (
        f"""
            WHERE
                afdtype in {afdtype}
        """ if afdtype else ""
    )
    departments = get_items_from_query(connection_string=connection_string, query=sql)
    return departments


def sd_enheder(connection_string: str, losid_tuple: tuple | None = None):
    """Get SDafdID from faellessql"""
    sql = """
        SELECT
            SDafdID, LOSID
        FROM
            [Personale].[sd].[Organisation]
    """
    sql += (
        f"""
            WHERE
                LOSID in {losid_tuple}
        """ if losid_tuple else ""
    )
    departments = get_items_from_query(connection_string=connection_string, query=sql)
    return departments


def af_losid(connection_str: str):
    """Get AF per LOSID"""
    sql = """
    SELECT
        v1.afdemail AS AF_email,
        v2.LOSID
    FROM
        (
        SELECT
            adm_faelles_id, lisid
        FROM
            [BuMasterdata].[dbo].[MD_ADM_FAELLESSKAB]
        WHERE
            STARTDATO <= GETDATE()
            and SLUTDATO > GETDATE()
        ) t
    LEFT JOIN
        [BuMasterdata].[dbo].[VIEW_MD_STAMDATA_AKTUEL] v1 ON t.adm_faelles_id = v1.lisid
    LEFT JOIN
        [BuMasterdata].[dbo].[VIEW_MD_STAMDATA_AKTUEL] v2 ON t.lisid = v2.lisid
    """
    af_email_kobling = get_items_from_query(connection_string=connection_str, query=sql)
    return af_email_kobling


def kv3_1_dev(connection_str: str, skole_afd: tuple, dagtilbud_afd: tuple, accept_ovk_dag: tuple, accept_ovk_skole: tuple):
    """Get wrong overenskomst in skole and dagtilbud respectively"""
    accept_dag_str = f"and Overenskomst not in {accept_ovk_dag}" if len(accept_ovk_dag) != 0 else ""
    accept_skole_str = f"and Overenskomst not in {accept_ovk_skole}" if len(accept_ovk_skole) != 0 else ""
    sql = f"""
        SELECT
            ans.Tjenestenummer, ans.Overenskomst, ans.Afdeling, ans.Institutionskode, perstam.Navn, ans.Startdato, ans.Slutdato, ans.Statuskode
        FROM
            [Personale].[sd_magistrat].[Ansættelse_mbu] ans
            left join [Personale].[sd].[personStam] as perstam
            on ans.CPR = perstam.CPR
        WHERE
            ((
                Afdeling in {dagtilbud_afd}
                and SUBSTRING(Overenskomst,1,1) = '7'
                and Overenskomst not in (76001, 76101)
                {accept_dag_str}
            )
            or
            (
                Afdeling in {skole_afd}
                and SUBSTRING(Overenskomst,1,1) = '4'
                and Overenskomst not in (46001, 46101)
                {accept_skole_str}
            ))
            and Statuskode in ('1', '3', '5')
            and Startdato <= GETDATE()
            and Slutdato > GETDATE()
    """
    items = get_items_from_query(connection_string=connection_str, query=sql)
    return items


def kv3_1(connection_str: str, skole_afd: tuple, dagtilbud_afd: tuple, accept_ovk_skole: tuple, accept_ovk_dag: tuple):
    """Get wrong overenskomst in skole and dagtilbud respectively"""
    accept_dag_str = f"and Overenskomst not in {accept_ovk_dag}" if len(accept_ovk_dag) != 0 else ""
    accept_skole_str = f"and Overenskomst not in {accept_ovk_skole}" if len(accept_ovk_skole) != 0 else ""
    sql = f"""
        SELECT
            ans.Tjenestenummer, ans.Overenskomst, ans.Afdeling, ans.Institutionskode, perstam.Navn, ans.Startdato, ans.Slutdato, ans.Statuskode
        FROM
            [Personale].[sd_magistrat].[Ansættelse_mbu] ans
            left join [Personale].[sd].[personStam] as perstam
            on ans.CPR = perstam.CPR
        WHERE
            ((
                Afdeling in {dagtilbud_afd}
                and Overenskomst in (76001, 76101, 77001)
                {accept_dag_str}
            )
            or
            (
                Afdeling in {skole_afd}
                and Overenskomst in (46001, 46101)
                {accept_skole_str}
            ))
            and Statuskode in ('1', '3', '5')
            and Startdato <= GETDATE()
            and Slutdato > GETDATE()
    """
    items = get_items_from_query(connection_string=connection_str, query=sql)
    return items


def kv4(orchestrator_connection: OrchestratorConnection, leder_overenskomst: tuple):
    """
    CASE: Ledere som mangler lås på anciennitetsdato.
    """

    connection_string_mbu = orchestrator_connection.get_constant("DbConnectionString").value
    connection_string_faelles = orchestrator_connection.get_constant("FaellesDbConnectionString").value
    sql = f"""
        SELECT
            ans.Tjenestenummer, ans.Overenskomst, ans.Afdeling, perstam.Navn, ans.Institutionskode,
            ans.Anciennitetsdato, org.LOSID

        FROM
            [Personale].[sd_magistrat].Ansættelse_mbu ans
            right join [Personale].[sd].[personStam] perstam
                on ans.CPR = perstam.CPR
            left join [Personale].[sd].[Organisation] org
                on ans.Afdeling = org.SDafdID
        WHERE
            ans.Overenskomst in {leder_overenskomst}
            and ans.Startdato <= GETDATE() and ans.Slutdato > GETDATE() and ans.Statuskode in ('1', '3', '5')
            and cast(ans.Anciennitetsdato as date) != '9999-12-31'
    """
    items = get_items_from_query(connection_string=connection_string_faelles, query=sql)
    item_df = pd.DataFrame(items).astype({'LOSID': int}, errors="ignore")

    af_email = af_losid(connection_str=connection_string_mbu)
    af_email_df = pd.DataFrame(af_email).astype({'LOSID': int}, errors="ignore")
    combined_df = pd.merge(left=item_df, right=af_email_df, on="LOSID")

    lis_dep = lis_enheder(connection_string=connection_string_mbu)
    lis_dep_df = pd.DataFrame(lis_dep).rename(columns={'losid': 'LOSID'}).astype({'LOSID': int}, errors="ignore")
    combined_df = pd.merge(left=combined_df, right=lis_dep_df, on="LOSID")

    items = list(combined_df.T.to_dict().values())
    return items


# Dictionary with process specific functions and parameters
PROCESS_PROCEDURE_DICT = {
        "KV1": {
            "procedure": kv1,
            "parameters": {"overenskomst": 47302},  # Overenskomst in which all employments should have INSTKODE = XC
        },
        "KV2": {
            "procedure": kv2,
            "parameters": {
                "tillaegsnr_par": tillaeg_pairs
            },
        },
        "KV3": {
            "procedure": kv3,
            "parameters": {
                "accept_ovk_dag": (),  # Overenskomster starting with "7" but accepted in dagtilbud/UIAA
                "accept_ovk_skole": (43011, 43017, 43031, 44001, 44101, 45001, 45002, 45081, 45082, 46901, 47591, 48888),  # Overenskomster starting with "4" but accepted in schools
            },
        },
        "KV3-DEV": {
            "procedure": kv3_dev,
            "parameters": {
                "accept_ovk_dag": (),
                "accept_ovk_skole": (43011, 43017, 43031, 44001, 44101, 45001, 45002, 45081, 45082, 46901, 47591, 48888),
            },
        },
        "KV4": {
            "procedure": kv4,
            "parameters": {
                "leder_overenskomst": (45082, 45081, 46901, 45101, 47201),
            },
        },
    }
