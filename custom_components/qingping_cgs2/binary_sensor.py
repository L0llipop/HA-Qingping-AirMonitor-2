import json
import logging
from homeassistant.components.binary_sensor import BinarySensorEntity
from homeassistant.components.sensor import RestoreEntity
from homeassistant.components import mqtt
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.const import EntityCategory

from .const import DOMAIN, CONF_MAC

_LOGGER = logging.getLogger(__name__)

async def async_setup_entry(hass, entry, async_add_entities):
    mac = entry.data[CONF_MAC]
    async_add_entities([
        QingpingBinarySensor(mac, "charging", "battery_charging"),
    ])

class QingpingBinarySensor(RestoreEntity, BinarySensorEntity):
    _attr_has_entity_name = True
    _attr_entity_category = EntityCategory.DIAGNOSTIC

    def __init__(self, mac, sensor_type, device_class):
        self._mac = mac
        self._sensor_type = sensor_type
        self._attr_unique_id = f"qingping_cgs2_{mac}_{sensor_type}"
        self._attr_translation_key = sensor_type # ПЕРЕВОД
        self._attr_device_class = device_class

        formatted_mac = ":".join(mac[i:i+2] for i in range(0, len(mac), 2))
        self._attr_device_info = DeviceInfo(
            identifiers={(DOMAIN, mac)},
            name="Qingping Air Monitor CGS2",
            manufacturer="Qingping",
            model="CGS2",
            connections={("mac", formatted_mac)},
        )

    async def async_added_to_hass(self):
        await super().async_added_to_hass()
        last_state = await self.async_get_last_state()
        if last_state and last_state.state in ["on", "off"]:
            self._attr_is_on = (last_state.state == "on")

        topic_up = f"qingping/{self._mac}/up"

        async def message_received(msg):
            try:
                payload = json.loads(msg.payload)
                msg_type = str(payload.get("type"))
                
                if msg_type == "17" and payload.get("need_ack") == 1:
                    ack_payload = json.dumps({"type": "17", "ack": 1})
                    topic_down = f"qingping/{self._mac}/down"
                    self.hass.async_create_task(mqtt.async_publish(self.hass, topic_down, ack_payload))
                
                if msg_type in ["12", "17"] and "sensorData" in payload:
                    payload["sensorData"].sort(key=lambda x: x["timestamp"]["value"], reverse=True)
                    latest_data = payload["sensorData"][0]
                    
                    bat_data = latest_data.get("battery", {})
                    status = bat_data.get("status")
                    level = bat_data.get("value")

                    if status is not None:
                        is_on = (status == 1 and level is not None and level < 99)
                        
                        if msg_type == "12":
                            self._attr_is_on = is_on
                            self.async_write_ha_state()
                        elif msg_type == "17" and getattr(self, "_attr_is_on", None) is None:
                            self._attr_is_on = is_on
                            self.async_write_ha_state()
            except Exception as e:
                _LOGGER.error("MQTT Binary Error: %s", e)

        await mqtt.async_subscribe(self.hass, topic_up, message_received)
