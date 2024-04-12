import json
from datetime import datetime
import traceback
import sys
import logging
import boto3
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3client = boto3.client('s3')
DATE = "%s-%02d-%02d" %(datetime.now().year, datetime.now().month, datetime.now().day)

#checker

def lambda_handler(event, context):
    bucket = "zadarastorage-metering"

    try:
        token = None
        count = 0

        obj_list = []
        while True:
            if token:
                response = s3client.list_objects_v2(Bucket=bucket, Prefix="ngos_report/", ContinuationToken=token)
            else:
                response = s3client.list_objects_v2(Bucket=bucket, Prefix="ngos_report/")

            for contents in response['Contents']:
                if 'zadara-qa' in contents['Key'] and DATE in contents['Key'] and '.json' in contents['Key']:
                    count += 1
                    obj_list.append(contents['Key'])

            if not response.get('IsTruncated'):
                break

            token = response.get('NextContinuationToken')

        #global_vars
        warning_count = 0
        bad_count = 0
        overall_data_disk_count = 0
        overall_metadata_disk_count = 0
        overall_data_disk_capacity = 0
        overall_data_disk_used_capacity = 0
        overall_metadata_disk_capacity = 0
        overall_metadata_disk_used_capacity = 0

        zios_list = []
        for obj in obj_list:
            data_disks = 0
            data_disks_mounted = 0
            metadata_disks = 0
            metadata_disks_mounted = 0
            proxy_vcs = 0
            storage_vcs = 0
            total_data_disk_capacity = 0
            total_data_disk_used_capacity = 0
            total_metadata_disk_capacity = 0
            total_metadata_disk_used_capacity = 0
            total_puts = 0
            total_data_errors_503 = 0
            total_metadata_errors_503 = 0
            total_osm_errors = 0
            audit_log_entire_size = 0
            proxy_ops_internal_log_size = {}
            proxy_ops_internal_log_count = {}
            proxy_ops_external_log_size = {}
            proxy_ops_external_log_count = {}
            zios_status = "good"

            response = s3client.get_object(Bucket=bucket, Key=obj)
            data = response['Body'].read().decode('utf-8')
            try:
                ngos_info = json.loads(data) #just to handle the error scenario if any part of script gets broken and there's no json data, not a big thing to consider
            except Exception:
                continue

            #zios_specific_data
            zios_name = ngos_info["zios"]["name"]
            zios_version = ngos_info["zios"]["version"]
            cloud_name = ngos_info["zios"]["cloud"]
            zios_cdate = ngos_info["zios"]["creation-date"]

            #disk_count_info, disk_mounted_info, disk_capacity_info
            for volume in ngos_info["disks"]:
                if ngos_info["disks"][volume]["disk_type"] == "Data":
                    data_disks += 1
                    if "mounted" in ngos_info["disks"][volume] and ngos_info["disks"][volume]["mounted"] == "yes": #there maybe possible machine failures during the time data collection script is invoked, better check if the maounted info is present in json file
                        data_disks_mounted += 1
                    if "used-capacity" in ngos_info["disks"][volume]:
                        total_data_disk_capacity += int(ngos_info["disks"][volume]["capacity"])
                        total_data_disk_used_capacity += float(ngos_info["disks"][volume]["used-capacity"])
                elif ngos_info["disks"][volume]["disk_type"] == "MetaData":
                    if ngos_info["disks"][volume]["port"] != "0": #there maybe a possiblity of devgroup in metadata disks which has port 0 (that has to be neglected!)
                        metadata_disks += 1
                        if "mounted" in ngos_info["disks"][volume] and ngos_info["disks"][volume]["mounted"] == "yes":
                            metadata_disks_mounted += 1
                    #sometimes in MetaDisks used capacity may be absent
                    if "used-capacity" in ngos_info["disks"][volume]:
                        total_metadata_disk_capacity += int(ngos_info["disks"][volume]["capacity"])
                        total_metadata_disk_used_capacity += float(ngos_info["disks"][volume]["used-capacity"])

            #vcs info
            for vc in ngos_info["vcs"]:
                if ngos_info["vcs"][vc]["role"] == "proxy-only":
                    proxy_vcs += 1
                elif ngos_info["vcs"][vc]["role"] == "proxy+storage":
                    storage_vcs += 1

            #function to check puts count
            def put_counter(type_of_operation, total_puts):
                for date in ngos_info["operations"][type_of_operation]:
                    put_count = ngos_info["operations"][type_of_operation][date]["PUT"]
                    if put_count.isdigit():
                        total_puts += int(put_count)
                return total_puts

            total_puts = put_counter("data_ops", total_puts)
            total_puts = put_counter("osm_ops", total_puts)

            for date in ngos_info["operations"]["osm_ops"]:
                if ngos_info["operations"]["osm_ops"][date]["PUT"].isdigit():
                    total_puts += int(ngos_info["operations"]["osm_ops"][date]["PUT"])

            #function to check errors
            def error_checker(type_of_error, total_errors, check_503=False):
                for date in ngos_info["operations"][type_of_error]:
                    for error in ngos_info["operations"][type_of_error][date]:
                        error_count = ngos_info["operations"][type_of_error][date][error]
                        if not check_503 or (check_503 and error == "Service-Unavailable(503)"):
                            if error_count.isdigit():  # Check if the values are numbers only
                                total_errors += int(error_count)
                return total_errors

            total_data_errors_503 = error_checker("data_errors", total_data_errors_503, check_503=True)

            total_metadata_errors_503 = error_checker("metadata_errors", total_metadata_errors_503, check_503=True)

            total_osm_errors = error_checker("osm_errors", total_osm_errors)

            #audit_log_info
            def bytes_to_gb(bytes):
                bytes = int(bytes)
                gb = bytes / (1024 ** 3)
                return round(gb, 2)

            audit_log_entire_size = bytes_to_gb(ngos_info["services"]["audit_log"]["audit_log_entire"]["size_used"])
            alm_status = ngos_info["services"]["audit_log"]["ALM"]["status"]

            for vc in ngos_info["services"]["audit_log"]["vc_logs"]:
                vc_log_info = ngos_info["services"]["audit_log"]["vc_logs"][vc]
                if "proxy_ops_internal.log" in vc_log_info:
                    proxy_ops_internal_log_size[vc] = bytes_to_gb(vc_log_info["proxy_ops_internal.log"]["size"])
                    if "count" in vc_log_info["proxy_ops_internal.log"]: #dummy condition for time being, will be deleted later
                        proxy_ops_internal_log_count[vc] = int(vc_log_info["proxy_ops_internal.log"]["count"])
                    else:
                        proxy_ops_internal_log_count[vc] = 1
                if "proxy_ops_external.log" in vc_log_info:
                    proxy_ops_external_log_size[vc] = bytes_to_gb(vc_log_info["proxy_ops_external.log"]["size"])
                    if "count" in vc_log_info["proxy_ops_external.log"]:
                        proxy_ops_external_log_count[vc] = int(vc_log_info["proxy_ops_external.log"]["count"])
                    else:
                        proxy_ops_external_log_count[vc] = 1

            #updating zios_status (warning)
            if total_data_errors_503 > 0 or total_metadata_errors_503 > 0:
                zios_status = "warning"
                warning_count += 1

            #updating zios_status (bad)
            if data_disks != data_disks_mounted or metadata_disks != metadata_disks_mounted or alm_status != "active" or audit_log_entire_size > 30 or any(size > 1 for size in proxy_ops_internal_log_size.values()) or any(count > 1 for count in proxy_ops_internal_log_count.values()) or any(size > 1 for size in proxy_ops_external_log_size.values()) or any(count > 1 for count in proxy_ops_internal_log_count.values()):
                zios_status = "bad"
                bad_count += 1

            zios_dict = {
                "zios_status": zios_status,
                "zios_name": zios_name,
                "zios_version": zios_version,
                "cloud_name": cloud_name,
                "creation_date": zios_cdate,
                "data_disks": data_disks,
                "data_disks_mounted": data_disks_mounted,
                "metadata_disks": metadata_disks,
                "metadata_disks_mounted":metadata_disks_mounted,
                "proxy_vcs": proxy_vcs,
                "storage_vcs": storage_vcs,
                "total_data_disk_capacity": total_data_disk_capacity,
                "total_data_disk_used_capacity": round(total_data_disk_used_capacity, 2),
                "total_metadata_disk_capacity": total_metadata_disk_capacity,
                "total_metadata_disk_used_capacity": round(total_metadata_disk_used_capacity, 2),
                "total_puts": total_puts,
                "total_data_errors_503": total_data_errors_503,
                "total_metadata_errors_503": total_metadata_errors_503,
                "total_osm_errors": total_osm_errors,
            }

            zios_list.append(zios_dict)

        #HTML table
        html_table = "<table border='1' id='ziosTable'><thead><tr>"
        #headers
        html_table += "<th><button class='headerDetails' title='status of zios' onclick='sortStatus()'>status</button></th>"
        html_table += "<th><button class='headerDetails' title='name of zios' onclick='sortText(1)'>zios_name</button></th>"
        html_table += "<th><button class='headerDetails' title='version of zios' onclick='sortText(2)'>zios_version</button></th>"
        html_table += "<th><button class='headerDetails' title='name of cloud' onclick='sortText(3)'>cloud_name</button></th>"
        html_table += "<th><button class='headerDetails' title='creation date of zios' onclick='sortByDate()'>creation_date</button></th>"
        html_table += "<th><button class='headerDetails' title='total number of data disks' onclick='sortNumeric(5)'>d_disks</button></th>"
        html_table += "<th><button class='headerDetails' title='total number of mounted data disks' onclick='sortNumeric(6)'>d_mtd</button></th>"
        html_table += "<th><button class='headerDetails' title='total number of metadata disks' onclick='sortNumeric(7)'>m_disks</button></th>"
        html_table += "<th><button class='headerDetails' title='total number of mounted metadata disks' onclick='sortNumeric(8)'>m_mtd</button></th>"
        html_table += "<th><button class='headerDetails' title='total number of proxy vcs' onclick='sortNumeric(9)'>p_vcs</button></th>"
        html_table += "<th><button class='headerDetails' title='total number of storage vcs' onclick='sortNumeric(10)'>s_vcs</button></th>"
        html_table += "<th><button class='headerDetails' title='total capacity of data disks' onclick='sortNumeric(11)'>d_capacity(GB)</button></th>"
        html_table += "<th><button class='headerDetails' title='used capacity of data disks' onclick='sortNumeric(12)'>d_used(GB)</button></th>"
        html_table += "<th><button class='headerDetails' title='total capacity of metadata disks' onclick='sortNumeric(13)'>m_capacity(GB)</button></th>"
        html_table += "<th><button class='headerDetails' title='used capacity of metadata disks' onclick='sortNumeric(14)'>m_used(GB)</button></th>"
        html_table += "<th><button class='headerDetails' title='total number of put operations' onclick='sortNumeric(15)'>put_ops</button></th>"
        html_table += "<th><button class='headerDetails' title='total number of 503 data errors' onclick='sortNumeric(16)'>d_errors(503)</button></th>"
        html_table += "<th><button class='headerDetails' title='total number of 503 metadata errors' onclick='sortNumeric(17)'>m_erros(503)</button></th>"
        html_table += "<th><button class='headerDetails' title='total number of osm errors' onclick='sortNumeric(18)'>osm_errors</button></th>"
        html_table += "</tr></thead><tbody>"
        #rows
        for zios in zios_list:
            #update data disks info
            overall_data_disk_count += zios["data_disks"]
            overall_data_disk_capacity += zios["total_data_disk_capacity"]
            overall_data_disk_used_capacity += zios["total_data_disk_used_capacity"]

            #update metadata disks info
            overall_metadata_disk_count += zios["metadata_disks"]
            overall_metadata_disk_capacity += zios["total_metadata_disk_capacity"]
            overall_metadata_disk_used_capacity += zios["total_metadata_disk_used_capacity"]

            #build html table
            html_table += "<tr>"
            for key, value in zios.items():
                if key == "zios_status":
                    if value == "good":
                        html_table += "<td style='color: #379237;'>" + str(value) + "</td>"
                    elif value == "warning":
                        html_table += "<td style='color: #FFC700;'>" + str(value) + "</td>"
                    else:
                        html_table += "<td style='color: #FE0000;'>" + str(value) + "</td>"
                else:
                    html_table += "<td>" + str(value) + "</td>"
            html_table += "</tr>"
        html_table += "</tbody></table>"

        #overview_data
        overview_data = "Count = {} (warning = {}, bad = {}), data_drives = {}, metadata_drives = {}, used/total_data_bytes = {}/{}, used/total_metadata_bytes = {}/{}".format(count, warning_count, bad_count, overall_data_disk_count, overall_metadata_disk_count, overall_data_disk_used_capacity, overall_data_disk_capacity, overall_metadata_disk_used_capacity, overall_metadata_disk_capacity)

        #append the table to the html and add sorting functionalities
        final_html_data = '''<!DOCTYPE html>
        <html>
        <head>
            <title>Zios Overall Report</title>
            <style>
            .headerDetails{
                font-weight: bold;
                border: none;
                background-color: #eee;
                transition: all ease-in-out 0.2s;
                cursor: pointer;
            }
            .headerDetails:hover{
                border: 1px solid #888;
                background-color: #ddd;
            }
            </style>
            <script>
                // Function to sort the table by status
                function sortStatus() {
                    var table, rows, switching, i, x, y, shouldSwitch;
                    table = document.getElementById("ziosTable");
                    switching = true;
                    while (switching) {
                        switching = false;
                        rows = table.rows;
                        for (i = 1; i < (rows.length - 1); i++) {
                            shouldSwitch = false;
                            x = rows[i].getElementsByTagName("td")[0]; // Index of the status column
                            y = rows[i + 1].getElementsByTagName("td")[0]; // Index of the status column
                            // Assign numerical values for sorting
                            var xValue = x.innerHTML === "bad" ? 0 : x.innerHTML === "warning" ? 1 : 2;
                            var yValue = y.innerHTML === "bad" ? 0 : y.innerHTML === "warning" ? 1 : 2;
                            if (xValue > yValue) {
                                shouldSwitch = true;
                                break;
                            }
                        }
                        if (shouldSwitch) {
                            rows[i].parentNode.insertBefore(rows[i + 1], rows[i]);
                            switching = true;
                        }
                    }
                }

                // Function to sort the table by text
                function sortText(columnIndex) {
                    var table, rows, switching, i, x, y, shouldSwitch;
                    table = document.getElementById("ziosTable");
                    switching = true;
                    while (switching) {
                        switching = false;
                        rows = table.rows;
                        for (i = 1; i < (rows.length - 1); i++) {
                            shouldSwitch = false;
                            x = rows[i].getElementsByTagName("td")[columnIndex];
                            y = rows[i + 1].getElementsByTagName("td")[columnIndex];
                            if (x.innerHTML.toLowerCase() > y.innerHTML.toLowerCase()) {
                                shouldSwitch = true;
                                break;
                            }
                        }
                        if (shouldSwitch) {
                            rows[i].parentNode.insertBefore(rows[i + 1], rows[i]);
                            switching = true;
                        }
                    }
                }

                // Function to sort the table by creation date
                function sortByDate() {
                    var table, rows, switching, i, x, y, shouldSwitch;
                    table = document.getElementById("ziosTable");
                    switching = true;
                    while (switching) {
                        switching = false;
                        rows = table.rows;
                        for (i = 1; i < (rows.length - 1); i++) {
                            shouldSwitch = false;
                            x = rows[i].getElementsByTagName("td")[4]; // Index of the creation date column
                            y = rows[i + 1].getElementsByTagName("td")[4]; // Index of the creation date column
                            if (new Date(x.innerHTML) < new Date(y.innerHTML)) {
                                shouldSwitch = true;
                                break;
                            }
                        }
                        if (shouldSwitch) {
                            rows[i].parentNode.insertBefore(rows[i + 1], rows[i]);
                            switching = true;
                        }
                    }
                }

                // Function to sort the table by numeric column
                function sortNumeric(columnIndex) {
                    var table, rows, switching, i, x, y, shouldSwitch;
                    table = document.getElementById("ziosTable");
                    switching = true;
                    while (switching) {
                        switching = false;
                        rows = table.rows;
                        for (i = 1; i < (rows.length - 1); i++) {
                            shouldSwitch = false;
                            x = rows[i].getElementsByTagName("td")[columnIndex];
                            y = rows[i + 1].getElementsByTagName("td")[columnIndex];
                            var xValue, yValue;
                            if (!isNaN(parseFloat(x.innerHTML)))  {
                                xValue = parseFloat(x.innerHTML);
                            } else {
                                xValue = parseInt(x.innerHTML);
                            }
                            if (!isNaN(parseFloat(y.innerHTML))) {
                                yValue = parseFloat(y.innerHTML);
                            } else {
                                yValue = parseInt(y.innerHTML);
                            }
                            if (xValue < yValue) {
                                shouldSwitch = true;
                                break;
                            }
                        }
                        if (shouldSwitch) {
                            rows[i].parentNode.insertBefore(rows[i + 1], rows[i]);
                            switching = true;
                        }
                    }
                }
            </script>
        </head>
        <body>
            %s
            <br>
            <h3>Summary:</h3>
            <p><pre>%s</pre></p>
        </body>
        </html>
        ''' % (html_table, overview_data)

        dest = "zadarastorage-ngos-daily-report"
        key = "qa/zios_overall_report_%s.html" % DATE

        s3client.put_object(Bucket=dest, Key=key, Body=final_html_data, ContentType='text/html')

        logger.info(zios_list)

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        line_number = exc_tb.tb_lineno
        print(f"An error occurred at line {line_number}: {e}")
        traceback.print_exc()

    return "Done"
