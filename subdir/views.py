import math
from urllib.parse import unquote
import datetime
from django.db import connection
from django.http import HttpResponse
import json

from django.views.decorators.csrf import csrf_exempt


def conv_util(o):
    if isinstance(o, (datetime.date, datetime.datetime)):
        return o.isoformat()


def conv_util2(o):
    if isinstance(o, datetime.timedelta):
        return o.__str__()


""" Zadanie 1 """


def health_print(request):
    with connection.cursor() as cursor:
        cursor.execute("SELECT date_trunc('second', current_timestamp - pg_postmaster_start_time()) as uptime;")
        resp = json.dumps({'pgsql': {'uptime': cursor.fetchone()}}, default=conv_util2)
        return HttpResponse(resp, content_type="application/json")


"""---------------------------------------------------------------------"""
""" Zadanie 2"""

DEF_PAGE = 1
DEF_PER_PAGE = 10
DEF_ORDER_BY = "registration_date"
DEF_ORDER_TYPE = "desc"


@csrf_exempt
def submissions(request):
    if request.method == "GET":
        return get_handler(request)
    if request.method == "POST":
        return pst_handler(request)
    if request.method == "DELETE":
        error = {
            "error": "missing submission id"
        }
        return HttpResponse(json.dumps(error),
                            content_type='application/json',
                            status=404)


""" DELETE part """


@csrf_exempt
def sub_delete(request, sub_id):
    query = "SELECT COUNT(*) " \
            "FROM ov.or_podanie_issues " \
            "WHERE id = %s;" % sub_id

    with connection.cursor() as cursor:
        cursor.execute(query)
        count = int(cursor.fetchone()[0])

    if count == 0:
        return del_return_error()

    query = "SELECT bulletin_issue_id, raw_issue_id " \
            "FROM ov.or_podanie_issues " \
            "WHERE id = %s;" % sub_id
    with connection.cursor() as cursor:
        cursor.execute(query)
        fetched = cursor.fetchall()
        bul_id = fetched[0][0]
        raw_id = fetched[0][1]

    query = "SELECT count(*) " \
            "FROM ov.or_podanie_issues " \
            "WHERE raw_issue_id = %s;" % raw_id
    with connection.cursor() as cursor:
        cursor.execute(query)
        raw_ct_podanie = int(cursor.fetchone()[0])

    query = "SELECT count (*) " \
            "FROM ov.or_podanie_issues " \
            "WHERE bulletin_issue_id = %s;" % bul_id
    with connection.cursor() as cursor:
        cursor.execute(query)
        bul_ct_podanie = int(cursor.fetchone()[0])

    query = "SELECT count (*) " \
            "FROM ov.raw_issues " \
            "WHERE bulletin_issue_id = %s;" % bul_id
    with connection.cursor() as cursor:
        cursor.execute(query)
        bul_ct_raw = int(cursor.fetchone()[0])

    print(bul_id, raw_id,
          raw_ct_podanie, bul_ct_podanie, bul_ct_raw)

    query = "DELETE FROM ov.or_podanie_issues " \
            "WHERE id = %s;" % sub_id
    with connection.cursor() as cursor:
        cursor.execute(query)

    if raw_ct_podanie == 1:
        query2 = "DELETE FROM ov.raw_issues " \
                 "WHERE id = %s; " % raw_id
        with connection.cursor() as cursor:
            cursor.execute(query2)

    if bul_ct_raw == 1 and bul_ct_podanie == 1:
        query3 = "DELETE FROM ov.bulletin_issues " \
                 "WHERE id = %s;" % bul_id
        with connection.cursor() as cursor:
            cursor.execute(query3)

    return HttpResponse("", status=204)


def del_return_error():
    error = {
        "error": {
            "message": "Záznam neexistuje"
        }
    }

    return HttpResponse(json.dumps(error),
                        content_type='application/json',
                        status=404)


""" GET part """


def conv_paging(page, per_page):
    try:
        page = (int(page))
    except:
        page = DEF_PAGE

    try:
        per_page = (int(per_page))
    except:
        per_page = DEF_PER_PAGE

    if per_page < 1:
        per_page = DEF_PER_PAGE
    if page < 1:
        page = DEF_PAGE

    limit = per_page
    offset = (page - 1) * per_page
    return limit, offset, page, per_page


def validate_order_by(order_by):
    if order_by in ["id", "br_court_name", "kind_name", "cin", "registration_date",
                    "corporate_body_name", "br_section", "br_insertion", "text",
                    "street", "postal_code", "city"]:
        return order_by
    else:
        return DEF_ORDER_BY


def validate_order_type(order_type):
    if order_type == 'desc' or order_type == 'asc':
        return order_type
    else:
        return DEF_ORDER_TYPE


def get_date_cond_string(date_lte, date_gte):
    try:
        date_lte_obj = datetime.datetime.fromisoformat(unquote(date_lte))
        date_lte_str = "%04d-%02d-%02d" % (date_lte_obj.year, date_lte_obj.month, date_lte_obj.day)
        lte_string = "registration_date <= '" + date_lte_str + "'"
    except:
        try:
            date_lte_obj = datetime.datetime.strptime(unquote(date_lte), '%Y-%m-%d %H:%M:%S.%f')
            date_lte_str = "%04d-%02d-%02d" % (date_lte_obj.year, date_lte_obj.month, date_lte_obj.day)
            lte_string = "registration_date <= '" + date_lte_str + "'"
        except:
            lte_string = ""

    try:
        date_gte_obj = datetime.datetime.fromisoformat(unquote(date_gte))
        date_gte_str = "%04d-%02d-%02d" % (date_gte_obj.year, date_gte_obj.month, date_gte_obj.day)
        gte_string = "registration_date >= '" + date_gte_str + "'"
    except:
        try:
            date_gte_obj = datetime.datetime.strptime(unquote(date_gte), '%Y-%m-%d %H:%M:%S.%f')
            date_gte_str = "%04d-%02d-%02d" % (date_gte_obj.year, date_gte_obj.month, date_gte_obj.day)
            gte_string = "registration_date >= '" + date_gte_str + "'"
        except:
            gte_string = ""

    return lte_string, gte_string


def get_query_search_string(searched, date_lte, date_gte):
    searched_cond = ""

    if searched != "":
        searched_cond = "(corporate_body_name iLIKE '%s' OR cin::varchar(255) iLIKE '%s' " \
                        "OR city iLIKE '%s')" % ('%' + searched + '%', '%' + searched + '%', '%' + searched + '%')

    lte_cond, gte_cond = get_date_cond_string(date_lte, date_gte)

    where_cond = ""
    conds = (searched_cond, lte_cond, gte_cond)
    first_cond = True

    for cond in conds:
        if cond == "":
            continue

        if first_cond:
            first_cond = False
            where_cond += "WHERE %s " % cond
        else:
            where_cond += "AND %s " % cond

    return where_cond


def dump_get_response(query_response, page, per_page, total):
    num_pages = math.ceil(total / per_page)

    big_data = {
        "items": [
            {
                "id": item[0],
                "br_court_name": item[1],
                "kind_name": item[2],
                "cin": item[3],
                "registration_date": item[4],
                "corporate_body_name": item[5],
                "br_section": item[6],
                "br_insertion": item[7],
                "text": item[8],
                "street": item[9],
                "postal_code": item[10],
                "city": item[11]
            }
            for item in query_response
        ],
        "metadata": {
            "page": page,
            "per_page": per_page,
            "pages:": num_pages,
            "total": total
        }
    }
    return json.dumps(big_data, default=conv_util)


def get_handler(request):
    page = request.GET.get("page", DEF_PAGE)
    per_page = request.GET.get("per_page", DEF_PER_PAGE)
    order_by = request.GET.get("order_by", "registration_date")
    order_type = request.GET.get("order_type", "desc")
    reg_lte = request.GET.get("registration_date_lte", "")
    reg_gte = request.GET.get("registration_date_gte", "")
    searched = unquote(request.GET.get("query", ""))

    limit, offset, page, per_page = conv_paging(page, per_page)
    order_by_checked = validate_order_by(order_by)
    order_type_checked = validate_order_type(order_type)
    query_search_string = get_query_search_string(searched, reg_lte, reg_gte)

    query = 'SELECT id, br_court_name, kind_name, cin, registration_date, ' \
            'corporate_body_name, br_section, br_insertion, text, street, postal_code, city \n' \
            'FROM ov.or_podanie_issues \n'

    query += query_search_string + '\n'
    query += 'ORDER BY ' + order_by_checked + ' ' + order_type_checked + '\n'
    query += 'LIMIT ' + str(limit) + '\n'
    query += 'OFFSET ' + str(offset) + ';\n'

    count_query = 'SELECT COUNT(*) FROM ov.or_podanie_issues\n'
    count_query += query_search_string + ';\n'

    print("\n")
    print('First query:\n' + query)
    print('Count query:\n' + count_query)
    with connection.cursor() as cursor:
        cursor.execute(query)
        temp1 = cursor.fetchall()
        cursor.execute(count_query)
        total = cursor.fetchall()[0][0]
        resp = dump_get_response(temp1, page, per_page, total)
        return HttpResponse(resp, content_type='application/json')


""" POST part """


def construct_error_json(errors):
    if (len(errors)) == 0:
        return None

    error = {"errors": [
        {
            "field": errors[2 * i],
            "reasons": [
                errors[2 * i + 1]
            ]
        }
        for i in range(len(errors) // 2)
    ]
    }

    return json.dumps(error)


def dump_post_response(podanie_id, court_name, kind_name, cin, reg_date, corp_name,
                       br_sec, text, street, postal_code, city):
    response = {"response": {
        "id": podanie_id,
        "br_court_name": court_name,
        "kind_name": kind_name,
        "cin": cin,
        "registration_date": reg_date,
        "corporate_body_name": corp_name,
        "br_section": br_sec,
        "text": text,
        "street": street,
        "postal_code": postal_code,
        "city": city
    }
    }
    return json.dumps(response)


def post_do_insertion(court_name, kind_name, cin, reg_date, corp_name, br_sec, br_insert,
                      text, street, postal_code, city):
    address = "%s, %s %s" % (street, postal_code, city)
    ct = datetime.datetime.now(datetime.timezone.utc)
    date = ("%04d-%02d-%02d %02d:%02d:%02d" % (ct.year, ct.month, ct.day, 0, 0, 0))
    date = datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
    date_time = "'%04d-%02d-%02d %02d:%02d:%02d.%06d'" % \
                (ct.year, ct.month, ct.day, ct.hour, ct.minute, ct.second, ct.microsecond)

    with connection.cursor() as cursor:
        query1 = "SELECT max(number)\n FROM ov.bulletin_issues WHERE year = %d;" % ct.year
        cursor.execute(query1)
        max_number = cursor.fetchone()[0]

    number = max_number + 1
    query1 = "INSERT INTO ov.bulletin_issues" \
             "(year, number, published_at, created_at, updated_at)\n" \
             "VALUES (%s, %s, '%s', %s, %s)\n" \
             "RETURNING id;\n" % \
             (str(ct.year), number, date.isoformat(), date_time, date_time)

    with connection.cursor() as cursor:
        cursor.execute(query1)
        bulletin_id = cursor.fetchone()[0]

    query2 = "INSERT INTO ov.raw_issues" \
             "(bulletin_issue_id, file_name, content, created_at, updated_at)\n" \
             "VALUES (%s, %s, %s, %s, %s)\n" \
             "RETURNING id;\n" % \
             (str(bulletin_id), "'-'", "'-'", date_time, date_time)

    with connection.cursor() as cursor:
        cursor.execute(query2)
        raw_id = cursor.fetchone()[0]

    query3 = "INSERT INTO ov.or_podanie_issues(bulletin_issue_id, raw_issue_id, br_mark, br_court_code, " \
             "br_court_name, kind_code, kind_name, cin, registration_date, corporate_body_name, br_section, " \
             "br_insertion, text, created_at, updated_at, address_line, street, postal_code, city) \n" \
             "VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %s, %s, '%s', '%s', '%s', '%s')" \
             "RETURNING id;\n" % \
             (str(bulletin_id), str(raw_id), "-", "-", court_name, "-", kind_name, str(cin), reg_date,
              corp_name, br_sec, br_insert, text, date_time, date_time, address, street, postal_code, city)

    print("\n")
    print("Query 1:\n" + query1)
    print("Query 2:\n" + query2)
    print("Query 3:\n" + query3)

    with connection.cursor() as cursor:
        cursor.execute(query3)
        podanie_id = cursor.fetchone()[0]
        return HttpResponse(dump_post_response(podanie_id, court_name, kind_name, cin, reg_date, corp_name,
                                               br_sec, text, street, postal_code, city),
                            content_type='application/json',
                            status=201)


def pst_parse_json(json_data):
    errors = []

    try:
        court_name = json_data['br_court_name']
    except:
        errors += ("br_court_name", "required")

    try:
        kind_name = json_data['kind_name']
    except:
        errors += ("kind_name", "required")

    try:
        cin = json_data['cin']
        try:
            cin = int(cin)
        except:
            errors += ("cin", "not_number")
    except:
        errors += ("cin", "required")

    try:
        reg_date = json_data['registration_date']
        try:
            date_gte_obj = datetime.datetime.fromisoformat(reg_date)
            date_gte_str = "%04d-%02d-%02d" % (date_gte_obj.year, date_gte_obj.month, date_gte_obj.day)
            gte_string = date_gte_str
            if date_gte_obj.year != datetime.datetime.now().year:
                errors += ("registration_date", "invalid range")
        except:
            try:
                date_gte_obj = datetime.datetime.strptime(reg_date, '%Y-%m-%d %H:%M:%S.%f')
                date_gte_str = "%04d-%02d-%02d" % (date_gte_obj.year, date_gte_obj.month, date_gte_obj.day)
                gte_string = date_gte_str
                if date_gte_obj.year != datetime.datetime.now().year:
                    errors += ("registration_date", "invalid range")
            except:
                errors += ("registration_date", "invalid format")

    except:
        errors += ("registration_date", "required")

    try:
        corp_name = json_data['corporate_body_name']
    except:
        errors += ("corporate_body_name", "required")

    try:
        br_sec = json_data['br_section']
    except:
        errors += ("br_section", "required")

    try:
        br_insert = json_data['br_insertion']
    except:
        errors += ("br_insertion", "required")

    try:
        text = json_data['text']
    except:
        errors += ("text", "required")

    try:
        street = json_data['street']
    except:
        errors += ("street", "required")

    try:
        postal_code = json_data['postal_code']
    except:
        errors += ("postal_code", "required")

    try:
        city = json_data['city']
    except:
        errors += ("city", "required")

    error = construct_error_json(errors)
    if error is not None:
        return error, None
    else:
        return None, post_do_insertion(court_name, kind_name, cin, gte_string, corp_name, br_sec,
                                       br_insert, text, street, postal_code, city)


def pst_handler(request):
    json_data = json.loads(request.body)

    error, response = pst_parse_json(json_data)
    if error is not None:
        return HttpResponse(error, content_type='application/json', status=422)
    else:
        return response


def del_handler(request):
    sub_id = request.get_raw_uri()
    print(sub_id)
    return HttpResponse("Delete")


def get_date_cond_string_z3(last_update_lte, last_update_gte):
    try:
        date_lte_obj = datetime.datetime.fromisoformat(unquote(last_update_lte))
        date_lte_str = "%04d-%02d-%02d" % (date_lte_obj.year, date_lte_obj.month, date_lte_obj.day)
        lte_string = "last_update <= '" + date_lte_str + "'"
    except:
        try:
            date_lte_obj = datetime.datetime.strptime(unquote(last_update_lte), '%Y-%m-%d %H:%M:%S.%f')
            date_lte_str = "%04d-%02d-%02d" % (date_lte_obj.year, date_lte_obj.month, date_lte_obj.day)
            lte_string = "last_update <= '" + date_lte_str + "'"
        except:
            lte_string = ""

    try:
        date_gte_obj = datetime.datetime.fromisoformat(unquote(last_update_gte))
        date_gte_str = "%04d-%02d-%02d" % (date_gte_obj.year, date_gte_obj.month, date_gte_obj.day)
        gte_string = "last_update >= '" + date_gte_str + "'"
    except:
        try:
            date_gte_obj = datetime.datetime.strptime(unquote(last_update_gte), '%Y-%m-%d %H:%M:%S.%f')
            date_gte_str = "%04d-%02d-%02d" % (date_gte_obj.year, date_gte_obj.month, date_gte_obj.day)
            gte_string = "last_update >= '" + date_gte_str + "'"
        except:
            gte_string = ""

    return lte_string, gte_string


def get_query_search_string_z3(searched, last_update_lte, last_update_gte):
    searched_cond = ""

    if searched != "":
        searched_cond = "(name iLIKE '%s' OR address_line iLIKE '%s')" \
                        % ('%' + searched + '%', '%' + searched + '%')

    lte_cond, gte_cond = get_date_cond_string_z3(last_update_lte, last_update_gte)

    where_cond = ""
    conds = (searched_cond, lte_cond, gte_cond)
    first_cond = True

    for cond in conds:
        if cond == "":
            continue

        if first_cond:
            first_cond = False
            where_cond += "WHERE %s " % cond
        else:
            where_cond += "AND %s " % cond

    return where_cond


def dump_get_response_z3(query_response, page, per_page, total):
    num_pages = math.ceil(total / per_page)

    big_data = {
        "items": [
            {
                "cin": item[0],
                "name": item[1],
                "br_section": item[2],
                "address_line": item[3],
                "last_update": item[4],
                "or_podanie_issues_count": item[5],
                "znizenie_imania_issues_count": item[6],
                "likvidator_issues_count": item[7],
                "konkurz_vyrovnanie_issues_count": item[8],
                "konkurz_restrukturalizacia_actors_count": item[9],
            }
            for item in query_response
        ],
        "metadata": {
            "page": page,
            "per_page": per_page,
            "pages:": num_pages,
            "total": total
        }
    }
    return json.dumps(big_data, default=conv_util)


def validate_order_by_z3(order_by):
    if order_by in ["cin", "name", "br_section", "address_line", "last_update",
                    "or_podanie_issues_count", "znizenie_imania_issues_count", "likvidator_issues_count",
                    "konkurz_vyrovnanie_issues_count", "konkurz_restrukturalizacia_actors_count"]:
        return order_by
    else:
        return "cin"


def z3(request):
    page = request.GET.get("page", DEF_PAGE)
    per_page = request.GET.get("per_page", DEF_PER_PAGE)
    order_by = request.GET.get("order_by", "cin")
    order_type = request.GET.get("order_type", "desc")
    last_update_lte = request.GET.get("last_update_lte", "")
    last_update_gte = request.GET.get("last_update_gte", "")
    searched = unquote(request.GET.get("query", ""))

    limit, offset, page, per_page = conv_paging(page, per_page)
    order_by_checked = validate_order_by_z3(order_by)
    order_type_checked = validate_order_type(order_type)
    query_search_string = get_query_search_string_z3(searched, last_update_lte, last_update_gte)

    query = 'with t_comp as ( \n' \
            'SELECT cin, name, br_section, address_line, last_update \n' \
            'FROM ov.companies \n'

    query += query_search_string + '\n'
    query += '),'

    query += \
        't_podanie as ( \n' \
        '   SELECT count(*) as or_podanie_issues_count, company_id \n' \
        '   from ov.or_podanie_issues \n' \
        '   group by company_id \n' \
        '), \n' \
        't_znizenie as ( \n' \
        '   SELECT count(*) as znizenie_imania_issues_count, company_id \n' \
        '   from ov.znizenie_imania_issues \n' \
        '   group by company_id \n' \
        '), \n' \
        't_likvidator as ( \n' \
        '   SELECT count(*) as likvidator_issues_count, company_id \n' \
        '   from ov.likvidator_issues \n' \
        '   group by company_id \n' \
        '), \n' \
        't_konkurz_issues as ( \n' \
        '   SELECT count(*) as konkurz_vyrovnanie_issues_count, company_id \n' \
        '   from ov.konkurz_vyrovnanie_issues \n' \
        '   group by company_id \n' \
        '), \n' \
        't_konkurz_actors as ( \n' \
        '   SELECT count(*) as konkurz_restrukturalizacia_actors_count, company_id \n' \
        '   from ov.konkurz_restrukturalizacia_actors \n' \
        '   group by company_id \n' \
        ') \n' \
        '' \
        ' SELECT t_comp.*, or_podanie_issues_count, znizenie_imania_issues_count, \n' \
        '   likvidator_issues_count, konkurz_vyrovnanie_issues_count, \n' \
        '   konkurz_restrukturalizacia_actors_count \n' \
        ' from t_comp \n' \
        ' left join t_podanie \n' \
        ' on t_comp.cin=t_podanie.company_id \n' \
        ' left join t_znizenie \n' \
        ' on t_comp.cin=t_znizenie.company_id \n' \
        ' left join t_likvidator \n' \
        ' on t_comp.cin=t_likvidator.company_id \n' \
        ' left join t_konkurz_issues \n' \
        ' on t_comp.cin=t_konkurz_issues.company_id \n' \
        ' left join t_konkurz_actors ' \
        ' on t_comp.cin=t_konkurz_actors.company_id \n'

    query += 'ORDER BY ' + order_by_checked + ' ' + order_type_checked + '\n'
    query += 'LIMIT ' + str(limit) + '\n'
    query += 'OFFSET ' + str(offset) + '\n'

    count_query = 'SELECT COUNT(*) FROM ov.companies\n'
    count_query += query_search_string + ';\n'

    print()
    print(query)
    print(count_query)

    #return HttpResponse("aa")

    with connection.cursor() as cursor:
        cursor.execute(query)
        temp1 = cursor.fetchall()
        cursor.execute(count_query)
        total = cursor.fetchall()[0][0]
        resp = dump_get_response_z3(temp1, page, per_page, total)
        return HttpResponse(resp, content_type='application/json')
