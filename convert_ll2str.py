import numpy as np

def get_cell_ids(dataset):
    """ Returns a list of the GloFAS ID for each cell in the given dataset

    Parameters
    ----------
    glofas_dataset : xarray.Dataset
        The Xarray dataset containing GloFAS cells to get IDs for

    Returns
    -------
    np.ndarray
        1D Numpy array containing the GloFAS cell IDs
    """
    cell_ids = np.asarray([build_cell_id(lat_val, lon_val)
                             for lat_val, lon_val in dataset])
    return cell_ids


def build_cell_id(lat_value, lon_value):
    """ Builds the GloFAS cell string ID, based on that cell's latitude and longitude value

    Parameters
    ----------
    lat_value : float
        Latitude of the GloFAS cell

    lon_value : float
        Longitude of the GloFAS cell

    Returns
    -------
    str
        The final string GloFAS cell ID
    """

    coords = (lat_value, lon_value)
    lat_string, lon_string = coord_values_as_alphanumeric_string(coords)
    cell_id = f"UK_{lat_string}_{lon_string}"
    return cell_id


def coord_values_as_alphanumeric_string(coord_value):
    """ Converts float signed coordinates (lat, lon) value to a string representation, using N, E, S, W notation
    and removing decimal point.
    Pads the digits to 3 numbers for the whole number part, and 2 numbers for the decimal part

    e.g. latitude of -56.2 would be represented by 05620S
         longitude of 125.05 would be represented by 12505E

    Parameters
    ----------
    coord_value : tuple
        Tuple of (lat_value, lon_value), with lat and lon as signed floating point types

    Returns
    -------
    tuple
        The alphanumeric string representation of the coords tuple
    """

    lat_value, lon_value = coord_value

    # pad the number to 3 whole number part, 2 decimal part
    lat_string = pad_and_stringify(lat_value, whole_part=3, decimal_part=2, remove_decimal_point=True)
    lon_string = pad_and_stringify(lon_value, whole_part=3, decimal_part=2, remove_decimal_point=True)

    # add the north or south qualifier
    if lat_value < 0:
        lat_string = f"{lat_string}S"
    else:
        lat_string = f"{lat_string}N"

    # add the east or west qualifier
    if lon_value < 0:
        lon_string = f"{lon_string}W"
    else:
        lon_string = f"{lon_string}E"

    return lat_string, lon_string


def pad_and_stringify(float_val, whole_part, decimal_part, remove_decimal_point):
    """ Pad the given float value to a given structure of "whole part.decimal part".  The whole part length will
    use zero (0) padding. Creates string of result, optionally removing decimal point.

    e.g. pad 95.5 to 3 whole number part, 2 decimal part, removing decimal point
    float_val=95.5, whole_part=3, decimal_part=2, remove_decimal_point=True
    result = "09550"

    Parameters
    ----------
    float_val : float
        Float value to pad and stringify

    whole_part : int
        How many digits to use for the whole part of the number, e.g. 0123.56, whole part is 4

    decimal_part : int
        How many digits to use after the decimal point, e.g. 123.50, decimal part is 2

    remove_decimal_point : bool
        Whether to remove the decimal point from the final string version of the float value

    Returns
    -------
    str
        String result of the padded value
    """
    # e.g. pad the number to 3 whole number part, 2 decimal part
    # including a place for the decimal point, this is 6 figures long, therefore 06.2f
    total_float_length = whole_part + decimal_part + 1
    pad_structure = f"0{total_float_length}.{decimal_part}f"

    # make absolute value to remove negative sign
    # and then remove the decimal point
    string_val = f"{abs(float_val):{pad_structure}}"
    if remove_decimal_point:
        string_val = string_val.replace('.', "")

    return string_val
