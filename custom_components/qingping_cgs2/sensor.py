import json
import logging
from datetime import datetime, timezone
from homeassistant.components.sensor import SensorEntity, RestoreSensor
from homeassistant.components.mqtt import async_subscribe
from homeassistant.helpers.entity import DeviceInfo
from homeassistant.const import EntityCategory
from homeassistant.components.recorder.statistics import async_add_external_statistics, StatisticData, StatisticMetaData

from .const import DOMAIN, CONF_MAC

_LOGGER = logging.getLogger(__name__)

# Словарь: только технические классы и единицы измерения
SENSORS = {
    "temperature": {"class": "temperature", "unit": "°C"},
    "humidity": {"class": "humidity", "unit": "%"},
    "co2": {"class": "carbon_dioxide", "unit": "ppm"},
    "noise": {"class": "sound_pressure", "unit": "dB"},
    "pm25": {"class": "pm25", "unit": "µg/m³"},
    "pm10": {"class": "pm10", "unit": "µg/m³"},
    "tvoc_index": {"class": None, "unit": None},
    "battery": {"class": "battery", "unit": "%", "cat": EntityCategory.DIAGNOSTIC},
    "power_mode": {"class": None, "unit": None, "cat": EntityCategory.DIAGNOSTIC},
}

async def async_setup_entry(hass, entry, async_add_entities):
    mac = entry.data[CONF_MAC]
    entities = [QingpingSensor(mac, key, data) for key, data in SENSORS.items()]
    async_add_entities(entities)

class QingpingSensor(RestoreSensor):
    _attr_has_entity_name = True

    def __init__(self, mac, sensor_key, sensor_data):
        self._mac = mac
        self._sensor_key = sensor_key
        self._attr_unique_id = f"qingping_cgs2_{mac}_{sensor_key}"
        
        # ПЕРЕВОД: Указываем ключ, по которому HA найдет имя в en.json / ru.json
        self._attr_translation_key = sensor_key 
        
        self._attr_device_class = sensor_data.get("class")
        self._attr_native_unit_of_measurement = sensor_data.get("unit")
        
        if sensor_key not in ["power_mode"]:
            self._attr_state_class = "measurement"
        
        if "cat" in sensor_data:
            self._attr_entity_category = sensor_data["cat"]

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
        last_state = await self.async_get_last_sensor_data()
        if last_state:
            self._attr_native_value = last_state.native_value

        topic = f"qingping/{self._mac}/up"

        async def message_received(msg):
            try:
                payload = json.loads(msg.payload)
                msg_type = str(payload.get("type"))
                
                if msg_type in ["12", "17"] and "sensorData" in payload:
                    sensor_data_list = payload["sensorData"]
                    if not sensor_data_list:
                        return
                        
                    sensor_data_list.sort(key=lambda x: x["timestamp"]["value"], reverse=True)
                    latest_data = sensor_data_list[0]

                    
                    if self._sensor_key == "power_mode":
                        status = latest_data.get("battery", {}).get("status")
                        if status is not None:
                            # Передаем системный ключ (mains/battery), а HA сам переведет его
                            self._attr_native_value = "mains" if status == 1 else "battery"
                            self.async_write_ha_state()
                        return

                    if self._sensor_key in latest_data:
                        sensor_info = latest_data[self._sensor_key]
                        if self._sensor_key == "battery" or sensor_info.get("status") == 0:
                            val = sensor_info.get("value")
                            if val is not None:
                                if self._sensor_key in ["temperature", "humidity"]:
                                    self._attr_native_value = round(float(val), 1)
                                else:
                                    self._attr_native_value = val
                                self.async_write_ha_state()

                    if msg_type == "17" and self._sensor_key not in ["power_mode", "battery"]:
                        hourly_buckets = {}
                        
                        for item in sensor_data_list:
                            if self._sensor_key in item:
                                val_info = item[self._sensor_key]
                                if val_info.get("status") == 0:
                                    val = val_info.get("value")
                                    ts_info = item.get("timestamp", {})
                                    ts = ts_info.get("value", 0) if isinstance(ts_info, dict) else int(item.get("timestamp", 0))
                                    
                                    if val is not None and ts > 0:
                                        hour_ts = ts - (ts % 3600)
                                        hourly_buckets.setdefault(hour_ts, []).append(float(val))
                        
                        if hourly_buckets:
                            statistics = []
                            for hour_ts in sorted(hourly_buckets):
                                values = hourly_buckets[hour_ts]
                                mean_val = sum(values) / len(values)
                                if self._sensor_key in ["temperature", "humidity"]:
                                    mean_val = round(mean_val, 1)
                                    
                                statistics.append(StatisticData(
                                    start=datetime.fromtimestamp(hour_ts, tz=timezone.utc),
                                    state=mean_val,
                                    mean=mean_val
                                ))
                            
                            metadata = StatisticMetaData(
                                has_mean=True,
                                has_sum=False,
                                name=f"Qingping CGS2 {self._sensor_key.upper()}",
                                source=DOMAIN,
                                statistic_id=f"{DOMAIN}:{self._mac.lower()}_{self._sensor_key}",
                                unit_of_measurement=self._attr_native_unit_of_measurement,
                            )
                            try:
                                async_add_external_statistics(self.hass, metadata, statistics)
                            except Exception as err:
                                _LOGGER.error("Statistics import error: %s", err)

            except Exception as e:
                _LOGGER.error("MQTT CGS2 Error: %s", e)

        await async_subscribe(self.hass, topic, message_received)
