
def convert_0_to_1(inp_obj):
    return {
        "file": inp_obj["data"]
    }

def convert_2_to_0(inp_obj):
    return {
        "data": inp_obj["text"]
    }

def convert_2_to_7(inp_obj):
    return {
        "text": [
            inp_obj["text"]
        ]
    }

def convert_8_to_7(inp_obj):
    return {
        "text": [
            " ".join(inp_obj["Paragraph Key phrase"])
        ]
    }

TYPE_TO_CONVERTER = {
    0: {
        1: convert_0_to_1
    },
    2: {
        0: convert_2_to_0,
        7: convert_2_to_7
    },
    8: {
        7: convert_8_to_7
    }
}