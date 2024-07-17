

def byte_to_string(hex_string):
    if hex_string == "0x":
        return ""
    # Remove the "0x" prefix and decode the hex string
    bytes_object = bytes.fromhex(hex_string[2:])
    try:
        human_readable_string = bytes_object.decode('utf-8')
    except UnicodeDecodeError:
        human_readable_string = bytes_object.decode('latin-1')
    return human_readable_string
