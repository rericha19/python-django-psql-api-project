import json
import math
from datetime import datetime, timezone
from urllib.parse import unquote

from django.db.models import Q, F, Max
from django.http import HttpResponse, JsonResponse
from .models import OrPodanieIssues, BulletinIssues
from .models import RawIssues
from . import views
from django.views.decorators.csrf import csrf_exempt


# v2/ov/submissions
@csrf_exempt
def submission(request):
    if request.method == "GET":
        return orm_get(request)
    if request.method == "POST":
        return orm_post(request)


@csrf_exempt
# v2/ov/submissions/id
def submission_id(request, sub_id):
    if request.method == "GET":
        return get_id(sub_id)
    if request.method == "DELETE":
        return delete_id(sub_id)
    if request.method == "PUT":
        return put_id(request, sub_id)


# ------------------------------------------------


def orm_check_date(date):
    try:
        date_lte_obj = datetime.fromisoformat(unquote(date))
        date_lte_str = "%04d-%02d-%02d" % (date_lte_obj.year, date_lte_obj.month, date_lte_obj.day)
    except:
        try:
            date_lte_obj = datetime.strptime(unquote(date), '%Y-%m-%d %H:%M:%S.%f')
            date_lte_str = "%04d-%02d-%02d" % (date_lte_obj.year, date_lte_obj.month, date_lte_obj.day)
        except:
            date_lte_str = None
    return date_lte_str


# GET   /v2/ov/submissions
def orm_get(request):
    page = request.GET.get("page", views.DEF_PAGE)
    per_page = request.GET.get("per_page", views.DEF_PER_PAGE)
    order_by = request.GET.get("order_by", "registration_date")
    order_type = request.GET.get("order_type", "desc")
    reg_lte = request.GET.get("registration_date_lte", "")
    reg_gte = request.GET.get("registration_date_gte", "")
    searched = unquote(request.GET.get("query", ""))

    limit, offset, page, per_page = views.conv_paging(page, per_page)
    order_by_checked = views.validate_order_by(order_by)
    order_type_checked = views.validate_order_type(order_type)

    reg_lte = orm_check_date(reg_lte)
    reg_gte = orm_check_date(reg_gte)

    qs = OrPodanieIssues.objects.all().values(
        'id', 'br_court_name', 'kind_name', 'cin',
        'registration_date', 'corporate_body_name',
        'br_section', 'br_insertion', 'text', 'street',
        'postal_code', 'city')

    if searched != "":
        qs = qs.filter(
            Q(corporate_body_name__icontains=searched) | Q(cin__icontains=searched) | Q(city__icontains=searched))

    if reg_lte is not None:
        qs = qs.filter(Q(registration_date__lte=reg_lte))
    if reg_gte is not None:
        qs = qs.filter(Q(registration_date__gte=reg_gte))

    if order_type_checked == "asc":
        qs = qs.order_by(F(order_by_checked).asc(nulls_last=True))
    else:
        qs = qs.order_by(F(order_by_checked).desc(nulls_last=True))

    total_count = qs.count()

    qs = qs[offset: offset + limit]
    num_pages = math.ceil(total_count / per_page)
    metadata = {
        "page": page,
        "per_page": per_page,
        "pages:": num_pages,
        "total": total_count
    }
    print(total_count)
    return JsonResponse({'items': list(qs), 'metadata': metadata}, status=200)


@csrf_exempt
def orm_post_do_insertion(court_name, kind_name, cin, reg_date, corp_name, br_sec, br_insert,
                          text, street, postal_code, city):
    address = "%s, %s %s" % (street, postal_code, city)
    ct = datetime.now(timezone.utc)
    date = ("%04d-%02d-%02d %02d:%02d:%02d" % (ct.year, ct.month, ct.day, 0, 0, 0))
    date = datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
    date_time = "'%04d-%02d-%02d %02d:%02d:%02d.%06d'" % \
                (ct.year, ct.month, ct.day, ct.hour, ct.minute, ct.second, ct.microsecond)

    number = BulletinIssues.objects.filter(year=ct.year).order_by("-number")[0].number + 1
    bulletin_new = BulletinIssues(year=ct.year, number=number, published_at=datetime.now(), created_at=datetime.now(), updated_at=datetime.now())
    bulletin_new.save()
    bull_id = bulletin_new.id

    raw_new = RawIssues(bulletin_issue_id=bull_id, file_name='-', content='-', created_at=datetime.now(), updated_at=datetime.now())
    raw_new.save()
    raw_id = raw_new.id

    podanie_new = OrPodanieIssues(bulletin_issue_id=bull_id, raw_issue_id=raw_id, br_mark='-', br_court_code='-',
                                  br_court_name=court_name, kind_code='-', kind_name=kind_name,
                                  cin=cin, registration_date=reg_date,
                                  corporate_body_name=corp_name, br_section=br_sec,
                                  br_insertion=br_insert, text=text, address_line=address,
                                  street=street, postal_code=postal_code, city=city,
                                  created_at=datetime.now(), updated_at=datetime.now())
    podanie_new.save()
    podanie_id = podanie_new.id
    return HttpResponse(views.dump_post_response(podanie_id, court_name, kind_name, cin, reg_date, corp_name,
                                                 br_sec, text, street, postal_code, city),
                        content_type='application/json',
                        status=201)


def orm_pst_parse_json(json_data):
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
            date_gte_obj = datetime.fromisoformat(reg_date)
            date_gte_str = "%04d-%02d-%02d" % (date_gte_obj.year, date_gte_obj.month, date_gte_obj.day)
            gte_string = date_gte_str
            if date_gte_obj.year != datetime.now().year:
                errors += ("registration_date", "invalid range")
        except:
            try:
                date_gte_obj = datetime.strptime(reg_date, '%Y-%m-%d %H:%M:%S.%f')
                date_gte_str = "%04d-%02d-%02d" % (date_gte_obj.year, date_gte_obj.month, date_gte_obj.day)
                gte_string = date_gte_str
                if date_gte_obj.year != datetime.now().year:
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

    error = views.construct_error_json(errors)
    if error is not None:
        return error, None
    else:
        return None, orm_post_do_insertion(court_name, kind_name, cin, gte_string, corp_name, br_sec,
                                           br_insert, text, street, postal_code, city)


# POST  /v2/ov/submissions
@csrf_exempt
def orm_post(request):
    json_data = json.loads(request.body)

    error, response = orm_pst_parse_json(json_data)
    if error is not None:
        return HttpResponse(error, content_type='application/json', status=422)
    else:
        return response


@csrf_exempt
# DEL   /v2/ov/submissions/id
def delete_id(sub_id):
    qs = OrPodanieIssues.objects.values().filter(id=sub_id).first()

    if qs is None:
        return views.del_return_error()

    bulletin_issue_id = qs['bulletin_issue_id']
    raw_issue_id = qs['raw_issue_id']

    raw_ct_podanie = OrPodanieIssues.objects.values().filter(raw_issue_id=raw_issue_id).count()
    bul_ct_podanie = OrPodanieIssues.objects.values().filter(bulletin_issue_id=bulletin_issue_id).count()
    bul_ct_raw = RawIssues.objects.values().filter(bulletin_issue_id=bulletin_issue_id).count()

    OrPodanieIssues.objects.filter(id=sub_id)[0].delete()
    if raw_ct_podanie == 1:
        RawIssues.objects.filter(id=raw_issue_id).delete()
    if bul_ct_raw == 1 and bul_ct_podanie == 1:
        BulletinIssues.objects.filter(id=bulletin_issue_id).delete()

    return HttpResponse("", status=204)


# GET   /v2/ov/submissions/id
def get_id(sub_id):
    qs = OrPodanieIssues.objects.values().filter(id=sub_id).first()
    if qs is None:
        return HttpResponse("[]")
    else:
        return HttpResponse(json.dumps(qs, default=views.conv_util), content_type='application/json')


# PUT   /v2/ov/submissions/id
def put_id(request, sub_id):
    json_data = json.loads(request.body)
    has_new = 0
    errors = []

    try:
        court_name = json_data['br_court_name']
        has_new = 1
    except:
        court_name = None

    try:
        kind_name = json_data['kind_name']
        has_new = 1
    except:
        kind_name = None

    try:
        cin = json_data['cin']
        try:
            cin = int(cin)
            has_new = 1
        except:
            errors += ("cin", "not_number")
    except:
        cin = None

    try:
        reg_date = json_data['registration_date']
        try:
            date_gte_obj = datetime.fromisoformat(reg_date)
            date_gte_str = "%04d-%02d-%02d" % (date_gte_obj.year, date_gte_obj.month, date_gte_obj.day)
            gte_string = date_gte_str
            if date_gte_obj.year != datetime.now().year:
                errors += ("registration_date", "invalid range")
            has_new = 1
        except:
            try:
                date_gte_obj = datetime.strptime(reg_date, '%Y-%m-%d %H:%M:%S.%f')
                date_gte_str = "%04d-%02d-%02d" % (date_gte_obj.year, date_gte_obj.month, date_gte_obj.day)
                gte_string = date_gte_str
                if date_gte_obj.year != datetime.now().year:
                    errors += ("registration_date", "invalid range")
                has_new = 1
            except:
                errors += ("registration_date", "invalid format")

    except:
        gte_string = None

    try:
        corp_name = json_data['corporate_body_name']
        has_new = 1
    except:
        corp_name = None

    try:
        br_sec = json_data['br_section']
        has_new = 1
    except:
        br_sec = None

    try:
        br_insert = json_data['br_insertion']
        has_new = 1
    except:
        br_insert = None

    try:
        text = json_data['text']
        has_new = 1
    except:
        text = None

    try:
        street = json_data['street']
        has_new = 1
    except:
        street = None

    try:
        postal_code = json_data['postal_code']
        has_new = 1
    except:
        postal_code = None

    try:
        city = json_data['city']
        has_new = 1
    except:
        city = None

    print(has_new)
    error = views.construct_error_json(errors)
    if error is not None:
        return HttpResponse(error,
                            content_type='application/json',
                            status=422)
    if has_new == 0:
        return HttpResponse("At least one change needed",
                            status=422)
    else:
        qs = OrPodanieIssues.objects.filter(id=sub_id).first()
        if court_name is not None:
            qs.br_court_name = court_name
        if kind_name is not None:
            qs.kind_name = kind_name
        if cin is not None:
            qs.cin = cin
        if gte_string is not None:
            qs.registration_date = gte_string
        if corp_name is not None:
            qs.corporate_body_name = corp_name
        if br_sec is not None:
            qs.br_section = br_sec
        if br_insert is not None:
            qs.br_insert = br_insert
        if text is not None:
            qs.text = text
        if street is not None:
            qs.street = street
        if postal_code is not None:
            qs.postal_code = postal_code
        if city is not None:
            qs.city = city

        qs = OrPodanieIssues.objects.values().filter(id=sub_id).first()
        return HttpResponse(json.dumps(qs, default=views.conv_util),
                            content_type='application/json',
                            status=201)


# GET   /v2/companies
def companies(request):
    return HttpResponse("not done sry")


