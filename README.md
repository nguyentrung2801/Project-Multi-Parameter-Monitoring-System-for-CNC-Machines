# Multi-Parameter Monitoring System for CNC Machines

## Overview
This project develops a multi-parameter monitoring system for CNC machines to collect and transmit real-time data for condition monitoring and future AI-based anomaly detection.

The system measures:
- Electrical parameters  
- Temperature  
- Vibration  
- Audio signals  

## Hardware
The system includes a central Master and multiple sensor nodes:
- Electrical node: PZEM-004T  
- Temperature node: PT100 + MAX31865  
- Vibration node: ADXL345  
- Audio node: ICS43434  

Communication is implemented via RS485 (RJ45 cable), with a 12V distributed power system. The Master collects data from all nodes and sends it to a PC via USB.

## Working Principle
Sensors collect data and send it to their respective nodes for processing. The system uses two communication buses:
- SLOW Bus for electrical and temperature data (low-speed, high reliability)  
- FAST Bus for vibration and audio data (high-speed streaming)  

The Master gathers data from both buses and transmits it to a PC, where Python software handles visualization and logging.

## Project Structure

```
.
├── Master/
│   ├── Master.ino
│   ├── fastbus.cpp / fastbus.h
│   ├── slowbus.cpp / slowbus.h
│   ├── usbmux.cpp / usbmux.h
│   └── common.h
│
├── Fast nodes/
│   ├── Audio_node.ino
│   └── Vibration_node.ino
│
├── Slow nodes/
│   ├── Electrical_node.ino
│   └── Temperature_node.ino
│
└── Python/
```
