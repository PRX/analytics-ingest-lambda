import ipaddr from "ipaddr.js";

/**
 * Sanity check ip addresses
 */
export const clean = (ipString) => {
  return cleanAll(ipString, false)[0];
};

export const cleanAll = (xffString, join = true) => {
  const parts = (xffString || "").split(",").map((s) => s.trim());
  const cleaned = parts.filter((s) => s && ipaddr.isValid(s));
  if (join) {
    return cleaned.join(", ") || undefined;
  } else {
    return cleaned;
  }
};

/**
 * Bitmask ip to remove last byte of v4, last half of v6
 */
export const mask = (cleanIpString) => {
  if (ipaddr.isValid(cleanIpString)) {
    const bytes = ipaddr.parse(cleanIpString).toByteArray();
    if (bytes.length === 4) {
      bytes[3] = 0;
    } else if (bytes.length === 16) {
      bytes[8] = 0;
      bytes[9] = 0;
      bytes[10] = 0;
      bytes[11] = 0;
      bytes[12] = 0;
      bytes[13] = 0;
      bytes[14] = 0;
      bytes[15] = 0;
    }
    return ipaddr.fromByteArray(bytes).toString();
  } else {
    return cleanIpString;
  }
};

export const maskLeft = (cleanXffString) => {
  const parts = (cleanXffString || "").split(", ");
  parts[0] = mask(parts[0]);
  return parts.join(", ");
};

/**
 * Some vendors can only handle IPv4 addresses, so send only those
 */
export const ipV4Only = (cleanIpString) => {
  if (ipaddr.isValid(cleanIpString)) {
    const ip = ipaddr.parse(cleanIpString);
    if (ip.kind() === "ipv4") {
      return cleanIpString;
    } else {
      return undefined;
    }
  } else {
    return undefined;
  }
};

/**
 * Convert to a fixed length string
 */
export const fixed = (cleanIpString) => {
  return fixedKind(cleanIpString)[0];
};

export const fixedKind = (cleanIpString) => {
  if (ipaddr.isValid(cleanIpString)) {
    const ip = ipaddr.parse(cleanIpString);
    if (ip.kind() === "ipv4") {
      return [ip.octets.map((n) => `00${n}`.substr(-3, 3)).join("."), "v4"];
    } else {
      return [ip.toFixedLengthString(), "v6"];
    }
  } else {
    return [cleanIpString, null];
  }
};
