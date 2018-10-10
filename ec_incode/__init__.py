__author__ = 'spowell'
import datetime
import logging

import arcpy

import ec_util
import usaddress



def read_address(_incode_file_path):
    READ_ERROR = -1
    READ_OK = 1
    READ_NOT = 0
    fields = [
        "current_consumption",
        "size_domain",
        "reading_status_domain",
        "reading_date",
        "active",
        "meter_number",
        "occupant",
        "address",
        "full_incode_account",
        "compound_meter_domain",
        "miu",
        "model",
        "current_meter_reading",
        "previous_meter_reading",
        "new_meter_reading",
        "new_miu",
        "meter_count",
        "account_meter_count"
    ]

    open_file = None
    edit = None
    new_meter_reading = None
    new_miu = None
    incode_file = _incode_file_path

    try:
        open_file = open(incode_file)
        records = open_file.readlines()
        address_list = []
        for rec in records:
            # miu
            miu = ec_util.to_pos_int_or_none(rec[28:38])
            if miu is None:
                continue
            #
            # read first read date and use for all input
            # if reading_date is None:
            reading_month = ec_util.to_pos_int_or_none(rec[108:110])
            reading_day = ec_util.to_pos_int_or_none(rec[110:112])
            reading_year = ec_util.to_pos_int_or_none(rec[112:114]) + 2000
            reading_date = datetime.date(reading_year, reading_month, reading_day)

            # active
            active = ec_util.to_upper_or_none(rec[159:160])
            if active is None:
                active = 'I'

            # current reading
            current_reading = None
            reading_status_domain = ec_util.to_reading_value(rec[70:78])
            #
            # check for reading errors based on input value
            if reading_status_domain == READ_ERROR:
                current_reading = None
                reading_status_domain = READ_ERROR
            elif reading_status_domain == 0:
                current_reading = None
                reading_status_domain = 0
            else:
                reading_status_domain = 1
                current_reading = ec_util.to_float_or_none(rec[70:78])

            previous_reading = None
            consumption = None
            if reading_status_domain == 1:
                previous_reading = ec_util.to_float_or_none(rec[162:177])
                if previous_reading:
                    consumption = current_reading - previous_reading
            # address
            address = ec_util.to_upper_or_none(rec[178:201])

            parsed_address = usaddress.parse(address)
            address_list.append(parsed_address)


        return address_list

    except IOError as e:
        logging.error(e)
        return

    except arcpy.ExecuteError as e:
        logging.error(e.message)
        return

    finally:
        if edit:
            edit.stopOperation()
            logging.info("Saving Changes")
            edit.stopEditing(save_changes=True)
        if open_file:
            open_file.close()


def load_incode_readings(_incode_path, _workspace):
    READ_ERROR = -1
    READ_OK = 1
    READ_NOT = 0
    arcpy.env.workspace = _workspace
    fields = [
        "current_consumption",
        "size_domain",
        "reading_status_domain",
        "reading_date",
        "active",
        "meter_number",
        "occupant",
        "address",
        "full_incode_account",
        "compound_meter_domain",
        "miu",
        "model",
        "current_meter_reading",
        "previous_meter_reading",
        "new_meter_reading",
        "new_miu",
        "meter_count",
        "account_meter_count"
    ]

    open_file = None
    edit = None
    new_meter_reading = None
    new_miu = None

    try:
        logging.info(arcpy.env.workspace)
        edit = arcpy.da.Editor(arcpy.env.workspace)
        edit.startEditing(with_undo=False, multiuser_mode=True)
        edit.startOperation()

        open_file = open(_incode_path)
        records = open_file.readlines()

        for rec in records:
            # miu
            miu = ec_util.to_pos_int_or_none(rec[28:38])
            if miu is None:
                continue
            #
            # read first read date and use for all input
            # if reading_date is None:
            reading_month = ec_util.to_pos_int_or_none(rec[108:110])
            reading_day = ec_util.to_pos_int_or_none(rec[110:112])
            reading_year = ec_util.to_pos_int_or_none(rec[112:114]) + 2000
            reading_date = datetime.date(reading_year, reading_month, reading_day)

            # active
            active = ec_util.to_upper_or_none(rec[159:160])
            if active is None:
                active = 'I'

            # current reading
            current_reading = None
            reading_status_domain = ec_util.to_reading_value(rec[70:78])
            #
            # check for reading errors based on input value
            if reading_status_domain == READ_ERROR:
                current_reading = None
                reading_status_domain = READ_ERROR
            elif reading_status_domain == 0:
                current_reading = None
                reading_status_domain = 0
            else:
                reading_status_domain = 1
                current_reading = ec_util.to_float_or_none(rec[70:78])

            previous_reading = None
            consumption = None
            if reading_status_domain == 1:
                previous_reading = ec_util.to_float_or_none(rec[162:177])
                if previous_reading:
                    consumption = current_reading - previous_reading
            # model
            model = ec_util.to_pos_int_or_none(rec[28:30])
            # address
            address = ec_util.to_upper_or_none(rec[178:201])
            # occupant
            occupant = ec_util.to_upper_or_none(rec[202:226])
            # complete incode account xx-xxxx-xx
            full_incode_account = ec_util.to_upper_or_none(rec[226:236])
            # incode account without the sequence xx-xxxx
            incode_account = ec_util.to_upper_or_none(rec[226:233])
            if incode_account == '12-2390' or incode_account == '07-0440' or incode_account == '02-1870':
                print incode_account
            # compound meters
            meter_count = ec_util.to_pos_long_or_none(rec[244:245])
            # account_meter_count
            account_meter_count = None
            if meter_count > 0:
                account_meter_count = full_incode_account + "-" + rec[243:245]

            if account_meter_count == '17-0930':
                print full_incode_account
            # current meter number
            meter_number = ec_util.to_pos_int_or_none(rec[278:467])
            # determine meter size
            size_domain = ec_util.to_pos_long_or_none(rec[278:280])
            compound_meter_domain = 0
            #
            # special case for multiple miu per account
            #
            if size_domain == 7L:
                compound_meter_domain = 1
            size_domain = ec_util.to_meter_size_domain(size_domain)
            #
            where = "incode_account = '{0}' AND meter_count = {1}".format(incode_account, meter_count)
            with arcpy.da.UpdateCursor(arcpy.env.workspace + "\\" + "Meter", fields, where) as cursor:
                for row in cursor:
                    row[0] = consumption
                    row[1] = size_domain
                    row[2] = reading_status_domain
                    row[3] = reading_date
                    row[4] = active
                    row[5] = meter_number
                    row[6] = occupant
                    row[7] = address
                    row[8] = full_incode_account
                    row[9] = compound_meter_domain
                    row[10] = str(miu)
                    row[11] = model
                    row[12] = current_reading
                    row[13] = previous_reading
                    row[14] = new_meter_reading
                    row[15] = new_miu
                    row[16] = meter_count
                    row[17] = account_meter_count
                    cursor.updateRow(row)
                    logging.debug(where)
    except IOError as e:
        logging.error(e)
        return

    except arcpy.ExecuteError as e:
        logging.error(e.message)
        return

    finally:
        if edit:
            edit.stopOperation()
            logging.info("Saving Changes")
            edit.stopEditing(save_changes=True)
        if open_file:
            open_file.close()
