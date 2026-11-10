import base64
import hashlib
import hmac
import json
import logging
import os
import re
import time

from lerai.config import int_env, required_env
from lerai.logging_utils import redact_value
from lerai.webex_presence import send_dm, get_sender_email, get_space_id, send_space_message


logger = logging.getLogger(__name__)
TOKEN_VERSION = "v2"
DEFAULT_PROMOTION_TOKEN_TTL_SECONDS = 3600
PROMOTE_USAGE = "Please use: `/promote approver=<name> token=<token>`"


def _load_requests():
    try:
        import requests
    except ModuleNotFoundError as exc:
        raise RuntimeError("Missing required dependency: requests") from exc
    return requests


def _load_webex_api():
    try:
        from webexteamssdk import WebexTeamsAPI
    except ModuleNotFoundError as exc:
        raise RuntimeError("Missing required dependency: webexteamssdk") from exc
    return WebexTeamsAPI


def _promotion_token_secret():
    return required_env("PROMOTION_TOKEN_SECRET").encode("utf-8")


def _promotion_token_ttl_seconds():
    return int_env("PROMOTION_TOKEN_TTL_SECONDS", DEFAULT_PROMOTION_TOKEN_TTL_SECONDS, minimum=1)


def _base64url_encode(raw_bytes):
    return base64.urlsafe_b64encode(raw_bytes).decode("utf-8").rstrip("=")


def _base64url_decode(value):
    missing_padding = len(value) % 4
    if missing_padding:
        value += "=" * (4 - missing_padding)
    return base64.urlsafe_b64decode(value)


def _sign_payload(payload):
    return _base64url_encode(hmac.new(_promotion_token_secret(), payload.encode("utf-8"), hashlib.sha256).digest())


def parse_promote_message(message):
    if not message:
        return None, None

    patterns = [
        r"(?:^|\s)/?promote\s+approver\s*[=:]\s*(?P<approver>\S+)\s+token\s*[=:]\s*(?P<token>\S+)",
        r"(?:^|\s)/?promote\s+ask\s+(?P<approver>\S+)\s+token\s+(?P<token>\S+)",
        r"(?:^|\s)/?promote\s+(?P<approver>\S+?)[,:]?\s+token\s*[=:]?\s*(?P<token>\S+)",
    ]

    for pattern in patterns:
        match = re.search(pattern, message.strip(), re.IGNORECASE)
        if match:
            return match.group("approver").strip().strip(",:"), match.group("token").strip().strip(",")

    return None, None


def _resolve_approver(approved_map, approver_input):
    if approver_input in approved_map:
        return approver_input, approved_map[approver_input]

    approver_lower = approver_input.lower()
    for name, email in approved_map.items():
        if name.lower() == approver_lower:
            return name, email

    return approver_input, None


def create_approval_token(sender_email, approver_email, webex_space, original_token):
    payload_data = {
        "version": TOKEN_VERSION,
        "sender": sender_email,
        "approver": approver_email,
        "webex_space": webex_space or "",
        "original_token": original_token,
        "timestamp": int(time.time()),
    }
    payload = _base64url_encode(json.dumps(payload_data, separators=(",", ":"), sort_keys=True).encode("utf-8"))
    signature = _sign_payload(payload)
    return f"{TOKEN_VERSION}.{payload}.{signature}"


def _is_token_fresh(timestamp, ttl_seconds=None):
    try:
        issued_at = int(timestamp)
    except (TypeError, ValueError):
        return False

    ttl = _promotion_token_ttl_seconds() if ttl_seconds is None else ttl_seconds
    now = int(time.time())
    return issued_at <= now and now - issued_at <= ttl

def decode_approval_token(token):
    """Validates and decodes a signed approval token."""
    try:
        version, payload, signature = token.split(".", 2)
        if version != TOKEN_VERSION:
            return None

        expected_signature = _sign_payload(payload)
        if not hmac.compare_digest(signature, expected_signature):
            return None

        decoded = json.loads(_base64url_decode(payload).decode("utf-8"))
        if decoded.get("version") != TOKEN_VERSION:
            return None
        if not _is_token_fresh(decoded.get("timestamp")):
            return None

        required_fields = ("sender", "approver", "webex_space", "original_token", "timestamp")
        if not all(field in decoded for field in required_fields):
            return None

        return {
            "sender": decoded["sender"],
            "approver": decoded["approver"],
            "webex_space": decoded["webex_space"],
            "original_token": decoded["original_token"],
            "timestamp": str(decoded["timestamp"]),
        }
    except Exception as e:
        logger.warning("Could not decode approval token: %s", e)
        
    return None


def handle_promotion_request(message, activity):
    """
    Main logic for processing a promotion request.
    Extracts approver/token and notifies the approver.
    """
    # 1. Setup & Authorization
    approved_users_raw = os.environ.get("APPROVED_USERS", {})
    if isinstance(approved_users_raw, str):
        try:
            approved_map = json.loads(approved_users_raw)
        except json.JSONDecodeError:
            return "❌ Error: APPROVED_USERS is not valid JSON."
    else:
        approved_map = approved_users_raw
    webex_token = os.environ.get("WEBEX_ACCESS_TOKEN")
    WebexTeamsAPI = _load_webex_api()
    api = WebexTeamsAPI(access_token=webex_token)
    
    sender_email = get_sender_email(activity)
    webex_space = get_space_id(activity)

    if sender_email not in approved_map.values():
        authorized_names = sorted(set(name for name in approved_map.keys() if "@" not in name))
        list_str = "\n".join([f"- {name}" for name in authorized_names])
        return (f"❌ Access Denied.\n\n**Authorized Requesters:**\n{list_str}")

    approver_input, token = parse_promote_message(message)

    if not approver_input or not token:
        return f"⚠️ Could not identify the approver or token. {PROMOTE_USAGE}"

    # 3. Resolve & Notify
    approver_name, target_email = _resolve_approver(approved_map, approver_input)
    if not target_email:
        return f"❌ `{approver_input}` is not an authorized approver."

    try:
        new_token = create_approval_token(sender_email, target_email, webex_space, token)
    except ValueError as e:
        return f"❌ Promotion token configuration error: {e}"

    if webex_space:
        dm_body = (
            f"🔔 **Promotion Request**\n\n"
            f"**Requester:** {sender_email}\n"
            f"**Approver:** <@personEmail:{target_email}>\n"
            f"Before approving, you may want to ask me to show you the latest offline-prod diff, verify that it's safe, and that the token matches {token}.\n"
            f"Once sure, to approve, reply to me with `/approve {new_token}` (DM or in the LeROY ops space)."
        )
        return dm_body
    else:
        dm_body = (
            f"🔔 **Promotion Request**\n\n"
            f"**Requester:** {sender_email}\n"
            f"**Approver:** {target_email}\n"
            f"Before approving, you may want to ask me to show you the latest offline-prod diff, verify that it's safe, and that the token matches {token}.\n"
            f"Once sure, to approve, reply to me with `/approve {new_token}` (DM or in the LeROY ops space)."
        )
        if send_dm(api, target_email, dm_body): #
            return f"✅ Request sent to **{approver_name}**. Awaiting approval."
        else:
            return f"❌ Failed to notify {target_email}."


def handle_approval_request(message, activity):

    #token_to_process = message.strip()
    token_to_process = message.strip().split()[-1]
    if not token_to_process:
            return "❌ Missing token. Usage: `/approve <token>`"

    # 1. Decode
    data = decode_approval_token(token_to_process)
    if not data:
        return "❌ Invalid, expired, or corrupted approval token."

    sender_email = get_sender_email(activity) #

    promotion_requester = data['sender']
    requested_approver = data['approver']
    original_token = data['original_token']
    webex_space = data['webex_space']
    
    # 2. Authorization Check
    if sender_email != requested_approver:
        return f"❌ Error: Approval messages come from {sender_email}, but original requester {promotion_requester} had asked {requested_approver} to approve the promotion. Approval message ignored."

    # 3. Request to LeROY Agent
    BASE = os.environ.get("FOOTPRINT_API_BASE_URL")
    cert_path = os.environ.get("CERT_PATH")
    key_path = os.environ.get("KEY_PATH")

    url = os.environ.get("LEROY_AGENT_PROMOTE_URL")
    params = {'token': original_token}

    webex_token = os.environ.get("WEBEX_ACCESS_TOKEN")
    requests = _load_requests()
    WebexTeamsAPI = _load_webex_api()
    api = WebexTeamsAPI(access_token=webex_token)

    try:
        response = requests.get(url, params=params, timeout=60, cert=(cert_path,key_path), verify=False)  # Set verify=True in production if you have a valid SSL certificate
        if response.status_code == 200:
            # 1. Parse the JSON body into a dictionary
            data = response.json() 
    
            # 2. Extract the stdout and stderr
            std_out = data.get("stdout")
            std_err = data.get("stderr")
    
            logger.info(
                "Promotion agent returned success",
                extra={"stdout": redact_value(std_out), "stderr": redact_value(std_err)},
            )
            
            # Who Am I responding to? 
            # 1. If request and approval both happened on the same space, reply once to that space (i.e. return the message with mentions). 
            # 2. If request and approval both happened on different spaces, reply once to each space (i.e. one space message, one return). 
            # 3. If request on space, and approver replied by DM, reply to the space and approver (i.e. one space message, one return)
            # 4. If request by DM, and approver replied to space, reply just once to the space (i.e. return the message with mentions). 
            # 5. If everything was on DM, reply to the approver and copy to requester by DM 

            current_webex_space = get_space_id(activity)
            msg_w_mention = (f"🚀 **Promotion Successful!**\n\n"
                   f"**Requester:** <@personEmail:{promotion_requester}>\n"
                   f"**Approver:** <@personEmail:{requested_approver}>\n"
                   f"**Promotion details:** {std_out}\n{std_err}")
            msg = (f"🚀 **Promotion Successful!**\n\n"
                   f"**Requester:** {promotion_requester}\n"
                   f"**Approver:** {requested_approver}\n"
                   f"**Promotion details:** {std_out}\n{std_err}")

            logger.info(
                "Promotion response routing resolved",
                extra={
                    "current_webex_space": redact_value(current_webex_space),
                    "request_webex_space": redact_value(webex_space),
                },
            )
            # 1. 
            if current_webex_space and webex_space and current_webex_space == webex_space:
                return msg_w_mention
            # 2. Reply to the leroy ops space and copy to approver
            if webex_space and current_webex_space and webex_space != current_webex_space:
                send_space_message(api, webex_space, msg_w_mention)
                return msg_w_mention
            # 3. Request on space, approval on DM
            if webex_space and not current_webex_space:
                send_space_message(api, webex_space, msg_w_mention)
                return msg
            # 4. Request on DM, approval on space
            if not webex_space and current_webex_space:
                return msg_w_mention
            # 5. Fully DM communication
            else:                
                if send_dm(api, promotion_requester, msg): 
                    msg = f"{msg} cc: {promotion_requester}."            
                    return msg
        else:
            msg = f"⚠️ Agent returned error ({response.status_code}): {response.text}"
            if send_dm(api, promotion_requester, msg): 
                msg = f"{msg} Informed promotion requester {promotion_requester}."
            return msg          
            
    except Exception as e:
        logger.exception("Promotion connection error")
        return f"🔥 Connection to LeROY Agent failed: `{str(e)}`"