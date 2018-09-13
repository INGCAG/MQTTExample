//
//  ViewController.swift
//  MQTTExample
//
//  Created by Cesar A. Guayara L. on 20/06/2017.
//  Copyright © 2017 INGCAG. All rights reserved.
//

import UIKit
//import MQTT

class ViewController: UIViewController {

    @IBOutlet weak var webView1: UIWebView!
    
    
    override func viewDidLoad() {
        super.viewDidLoad()
        // Do any additional setup after loading the view, typically from a nib.
        var MQTTClient: MQTT;
        MQTTClient = MQTT(clientID: "iOS_Example", host: "m23.cloudmqtt.com", port: 17126);
        MQTTClient.ping();
        
    }

    override func didReceiveMemoryWarning() {
        super.didReceiveMemoryWarning()
        // Dispose of any resources that can be recreated.
    }
    
    @IBAction func btnOn(_ sender: Any) {
        var MQTTClient: MQTT;
        var MQTTMssg: MQTTMessage;
        
        //MQTTClient = MQTT(clientID: "iOS_Example", host: "tcp://localhost", port: 1883);
        MQTTClient = MQTT(clientID: "iOS_Example", host: "m23.cloudmqtt.com", port: 17126);
        print(MQTTClient.connect());
        
        MQTTMssg = MQTTMessage(topic: "MQTT/Cesar", string: "Message from SimpleMqttIOSClient");
        print(MQTTMssg.payload);
        
        MQTTClient.publish(MQTTMssg)
        MQTTClient.subscribe("MQTT/Cesar");
        
    }

    @IBAction func btnOff(_ sender: Any) {
        var MQTTClient: MQTT;
        MQTTClient = MQTT(clientID: "iOS_Example", host: "m23.cloudmqtt.com", port: 17126);
        MQTTClient.disconnect();
    }

}

