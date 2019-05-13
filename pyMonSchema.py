# -*- coding:utf-8 -*-
import mongoDBM
import logging
import json
import os
import wx
# from bson.json_util import loads

class PyMonSchemaFrame(wx.Frame):
    """Another free MongoDB schema analyser based on [PyMongo](https://github.com/mongodb/mongo-python-driver) and [wxPython](https://github.com/wxWidgets/wxPython)
    """
    def __init__(self, *args, **kw):
        # ensure the parent's __init__ is called
        super(PyMonSchemaFrame, self).__init__(*args, size=(1050, 600), **kw)
        self.dbm = None
        # create a panel in the frame
        panel = wx.Panel(self)
        self.panel = panel

        # multi-line text
        self.mongo_uri_TextCtrl = wx.TextCtrl(panel, style=wx.TE_CENTER)
        self.mongo_db_TextCtrl = wx.TextCtrl(panel, style=wx.TE_CENTER)
        self.mongo_coll_TextCtrl = wx.TextCtrl(panel, style=wx.TE_CENTER)

        self.mongo_connect_button = wx.Button(panel, label=u"Connect!")
        self.mongo_connect_button.SetBackgroundColour( wx.SystemSettings.GetColour( wx.SYS_COLOUR_ACTIVECAPTION ) )
        self.mongo_switch_button = wx.Button(panel, label=u"Switch!")
        self.mongo_switch_button.SetBackgroundColour( wx.SystemSettings.GetColour( wx.SYS_COLOUR_ACTIVECAPTION ) )
        self.Bind(wx.EVT_BUTTON, self.OnConnect, self.mongo_connect_button)
        self.Bind(wx.EVT_BUTTON, self.OnSwitch, self.mongo_switch_button)

        self.query_TextCtrl = wx.TextCtrl(panel)
        orderChoices = ["Positive", "Negative"]
        self.order_Choice = wx.Choice(panel, wx.ID_ANY, wx.DefaultPosition, wx.DefaultSize, orderChoices, 0)
        self.order_Choice.SetSelection(0)

        self.limit_TextCtrl = wx.TextCtrl(panel)
        self.omit_keys_TextCtrl = wx.TextCtrl(panel)
        self.omit_patterns_TextCtrl = wx.TextCtrl(panel)
        self.embed_CheckBox = wx.CheckBox(panel)
        self.analyse_button = wx.Button(panel, label=u"Analyse!")
        self.analyse_button.SetBackgroundColour( wx.SystemSettings.GetColour( wx.SYS_COLOUR_ACTIVECAPTION ) )
        self.Bind(wx.EVT_BUTTON, self.OnSchemaAnalyser, self.analyse_button)

        self.save_button = wx.Button( panel, wx.ID_ANY, u"Save", wx.DefaultPosition, wx.Size( 50,25 ), 0 )
        self.save_button.SetBackgroundColour( wx.SystemSettings.GetColour( wx.SYS_COLOUR_ACTIVECAPTION ) )
        self.Bind(wx.EVT_BUTTON, self.save_file_content, self.save_button)

        self.list = wx.ListCtrl(panel, wx.ID_ANY, wx.DefaultPosition, wx.DefaultSize, wx.LC_REPORT | wx.LC_VRULES)
        self.list.InsertColumn(0, "number")
        self.list.InsertColumn(1, "key")
        self.list.InsertColumn(2, "total_occurrence")
        self.list.InsertColumn(3, "type(occurrence-percent)")

        self.list.SetColumnWidth(0, 60)  
        self.list.SetColumnWidth(1, 180)
        self.list.SetColumnWidth(2, 180)
        self.list.SetColumnWidth(3, 360)

        self.rst_write_str = ""
        self.db = ""
        self.coll = ""
        # box sizer
        self.makeBoxSizer()

        # create a menu bar
        self.makeMenuBar()

        # and a status bar
        self.CreateStatusBar()
        self.SetStatusText("Welcome to Schema Analyser!")
        self.initiateValue()

    def initiateValue(self):
        self.mongo_uri_TextCtrl.SetValue("mongodb://localhost:27017/admin")
        self.mongo_db_TextCtrl.SetValue("test")
        self.mongo_coll_TextCtrl.SetValue("test")

    def makeBoxSizer(self):
        box = wx.BoxSizer()  
        First = wx.StaticText(self.panel, wx.ID_ANY, u"First:", wx.DefaultPosition, wx.DefaultSize, 0 )
        First.SetFont( wx.Font( 12, wx.FONTFAMILY_MODERN, wx.FONTSTYLE_NORMAL, wx.FONTWEIGHT_BOLD, False, "Consolas" ) )
        First.Wrap( -1 )
        First.SetForegroundColour( wx.Colour( 0, 128, 255 ) )
        First.SetBackgroundColour( wx.SystemSettings.GetColour( wx.SYS_COLOUR_BTNHIGHLIGHT ) )

        Second = wx.StaticText(self.panel, wx.ID_ANY, u"Second:", wx.DefaultPosition, wx.DefaultSize, 0 )
        Second.SetFont( wx.Font( 12, wx.FONTFAMILY_MODERN, wx.FONTSTYLE_NORMAL, wx.FONTWEIGHT_BOLD, False, "Consolas" ) )
        Second.Wrap( -1 )
        Second.SetForegroundColour( wx.Colour( 0, 128, 255 ) )
        Second.SetBackgroundColour( wx.SystemSettings.GetColour( wx.SYS_COLOUR_BTNHIGHLIGHT ) )

        uriText = wx.StaticText(self.panel, label=u"MongoDB_URI*:")
        dbText = wx.StaticText(self.panel, label=u"Database*:")
        collText = wx.StaticText(self.panel, label=u"Collection*:")
        static_line_1 = wx.StaticLine(self.panel, wx.ID_ANY, wx.DefaultPosition, wx.DefaultSize, wx.LI_HORIZONTAL)
        

        query = wx.StaticText(self.panel, label=u"Query Document:")
        order = wx.StaticText(self.panel, label=u"Order:")
        limit = wx.StaticText(self.panel, label=u"Limit:")
        omit_keys = wx.StaticText(self.panel, label=u"Omit_keys:")
        omit_patterns = wx.StaticText(self.panel, label=u"Omit_patterns:")
        embed = wx.StaticText(self.panel, label=u"Embed-keys:")

        static_line_2 = wx.StaticLine(self.panel, wx.ID_ANY, wx.DefaultPosition, wx.DefaultSize, wx.LI_HORIZONTAL)

        Output = wx.StaticText(self.panel, wx.ID_ANY, u"Output:", wx.DefaultPosition, wx.DefaultSize, 0 )
        Output.SetFont( wx.Font( 12, wx.FONTFAMILY_MODERN, wx.FONTSTYLE_NORMAL, wx.FONTWEIGHT_BOLD, False, "Consolas" ) )
        Output.Wrap( -1 )
        Output.SetForegroundColour( wx.Colour( 0, 128, 255 ) )
        Output.SetBackgroundColour( wx.SystemSettings.GetColour( wx.SYS_COLOUR_BTNHIGHLIGHT ) )

        box.Add(uriText, proportion=0, flag=wx.ALL | wx.ALIGN_CENTER_VERTICAL, border=3)
        box.Add(self.mongo_uri_TextCtrl, proportion=1, flag=wx.EXPAND | wx.ALL | wx.ALIGN_CENTER_VERTICAL, border=5)
        box.Add(self.mongo_connect_button, proportion=0, flag=wx.ALL | wx.ALIGN_CENTER_VERTICAL, border=8)

        box2 = wx.BoxSizer()
        box2.Add(dbText, proportion=0, flag=wx.ALL | wx.ALIGN_CENTER_VERTICAL, border=3) 
        box2.Add(self.mongo_db_TextCtrl, proportion=1, flag=wx.EXPAND | wx.ALL | wx.ALIGN_CENTER_VERTICAL, border=5)  
        box2.Add(collText, proportion=0, flag=wx.ALL | wx.ALIGN_CENTER_VERTICAL, border=3)
        box2.Add(self.mongo_coll_TextCtrl, proportion=1, flag=wx.EXPAND | wx.ALL | wx.ALIGN_CENTER_VERTICAL, border=5) 
        box2.Add(self.mongo_switch_button, proportion=0, flag=wx.ALL | wx.ALIGN_CENTER_VERTICAL, border=8) 

        box3 = wx.BoxSizer()
        box3.Add(query, proportion=0, flag=wx.ALL | wx.ALIGN_CENTER_VERTICAL, border=3) 
        box3.Add(self.query_TextCtrl, proportion=1, flag=wx.EXPAND | wx.ALL | wx.ALIGN_CENTER_VERTICAL, border=5)  
        box3.Add(order, proportion=0, flag=wx.ALL | wx.ALIGN_CENTER_VERTICAL, border=3) 
        box3.Add(self.order_Choice, proportion=1, flag=wx.EXPAND | wx.ALL | wx.ALIGN_CENTER_VERTICAL, border=5) 
        box3.Add(limit, proportion=0, flag=wx.ALL | wx.ALIGN_CENTER_VERTICAL, border=3)  
        box3.Add(self.limit_TextCtrl, proportion=1, flag=wx.EXPAND | wx.ALL | wx.ALIGN_CENTER_VERTICAL, border=5) 
        box3.Add(omit_keys, proportion=0, flag=wx.ALL | wx.ALIGN_CENTER_VERTICAL, border=3) 
        box3.Add(self.omit_keys_TextCtrl, proportion=1, flag=wx.EXPAND | wx.ALL | wx.ALIGN_CENTER_VERTICAL, border=5)  
        box3.Add(omit_patterns, proportion=0, flag=wx.ALL | wx.ALIGN_CENTER_VERTICAL, border=3) 
        box3.Add(self.omit_patterns_TextCtrl, proportion=1, flag=wx.EXPAND | wx.ALL | wx.ALIGN_CENTER_VERTICAL, border=5) 
        box3.Add(embed, proportion=0, flag=wx.ALL | wx.ALIGN_CENTER_VERTICAL, border=3)  
        box3.Add(self.embed_CheckBox, proportion=1, flag=wx.EXPAND | wx.ALL | wx.ALIGN_CENTER_VERTICAL, border=5) 


        v_box = wx.BoxSizer(wx.VERTICAL) 
        v_box.Add(First, proportion=0, flag=wx.ALL | wx.ALIGN_CENTER_VERTICAL, border=3) 
        v_box.Add(box, proportion=0, flag=wx.EXPAND | wx.ALL | wx.ALIGN_CENTER_VERTICAL, border=3) 
        v_box.Add(box2, proportion=0, flag=wx.EXPAND | wx.ALL | wx.ALIGN_CENTER_VERTICAL, border=3) 
        v_box.Add(static_line_1, proportion=0, flag=wx.EXPAND | wx.ALL | wx.ALIGN_CENTER_VERTICAL, border=3)  

        v_box.Add(Second, proportion=0, flag=wx.ALL | wx.ALIGN_CENTER_VERTICAL, border=3) 
        v_box.Add(box3, proportion=0, flag=wx.EXPAND | wx.ALL | wx.ALIGN_CENTER_VERTICAL, border=3)
        v_box.Add(self.analyse_button, proportion=0, flag=wx.ALL | wx.ALIGN_CENTER_HORIZONTAL, border=3) 
        v_box.Add(static_line_2, proportion=0, flag=wx.EXPAND | wx.ALL | wx.ALIGN_CENTER_VERTICAL, border=3) 

        v_box.Add(Output, proportion=0, flag=wx.ALL | wx.ALIGN_CENTER_VERTICAL, border=3) 
        v_box.Add(self.save_button, proportion=0, flag=wx.ALL, border=0) 
        v_box.Add(self.list, proportion=5, flag=wx.EXPAND | wx.ALL | wx.ALIGN_CENTER_VERTICAL, border=5) 

        self.panel.SetSizer(v_box)  

    def makeMenuBar(self):
        """
        A menu bar is composed of menus, which are composed of menu items.
        This method builds a set of menus and binds handlers to be called
        when the menu item is selected.
        """

        # Make a file menu with Hello and Exit items
        fileMenu = wx.Menu()
        # The "\t..." syntax defines an accelerator key that also triggers
        # the same event
        helloItem = fileMenu.Append(-1, "&Hello...\tCtrl-H",
                                    "Help string shown in status bar for this menu item")
        fileMenu.AppendSeparator()
        # When using a stock ID we don't need to specify the menu item's
        # label
        exitItem = fileMenu.Append(wx.ID_EXIT)

        # Now a help menu for the about item
        helpMenu = wx.Menu()
        operation_instruction_item = helpMenu.Append(-1, "&Operation Instruction...\tCtrl-H",
                                    "Show Detailed operation instruction.")
        aboutItem = helpMenu.Append(wx.ID_ABOUT)
        exitItem = helpMenu.Append(wx.ID_EXIT)

        # Make the menu bar and add the two menus to it. The '&' defines
        # that the next letter is the "mnemonic" for the menu item. On the
        # platforms that support it those letters are underlined and can be
        # triggered from the keyboard.
        menuBar = wx.MenuBar()
        # menuBar.Append(fileMenu, "&File")
        menuBar.Append(helpMenu, "&Help")

        # Give the menu bar to the frame
        self.SetMenuBar(menuBar)

        # Finally, associate a handler function with the EVT_MENU event for
        # each of the menu items. That means that when that menu item is
        # activated then the associated handler function will be called.
        self.Bind(wx.EVT_MENU, self.OnOI, operation_instruction_item)
        self.Bind(wx.EVT_MENU, self.OnExit, exitItem)
        self.Bind(wx.EVT_MENU, self.OnAbout, aboutItem)

    def OnExit(self, event):
        """Close the frame, terminating the application."""
        self.Close(True)

    def OnOI(self, event):
        """Say hello to the user."""
        wx.MessageBox(
        '''Operation Instruction:\n
First:
Fill MongoDB uri and click Connect button to connect MongoDB deployment. 
Fill database name and collection name to switch to specific collection.
MongoDB uri, database name and collection name are all required field.

Second:
After connect and switch to specific collection, there are several optional fields before analyse schema.
Query -> MongoDB query document to filter input to analyse.
Order -> Positive/Negative, used in sort document, order=Positive equivalent to sort("_id":1), order=Negative equivalent to sort("_id":-1).
Limit -> Int, limit value of query result. Default is 0, which means no limit.
Omit_keys -> Fields string to be omitted, sperate by comma. such as: keyName1, keyName2 .
Omit_patterns -> Fileds match these regular expression patterns will be omitted, sperate by comma. such as: ^keyNameHead, keyNameTail$ .
Embed-keys -> Whether to analyse embed-key (e.g. keyNameParent.keyNameChild1.keyNameChild2) or not.
Analyse -> Run analyse.

Output:
Display schema result.
Save button -> save the result as a json file named databaseName_collectionName-Schema.json default.         
        '''
            )

    def OnAbout(self, event):
        """Display an About Dialog"""
        wx.MessageBox("MongoDB schema analyser",
                      "MongoDB schema analyser",
                      wx.OK | wx.ICON_INFORMATION)

    def ReportMsg(self, msg, status):
        wx.MessageBox(msg, status, wx.OK | wx.ICON_INFORMATION)
        self.SetStatusText(msg)
        print(msg)

    def OnConnect(self, event):
        try:
            uri = self.mongo_uri_TextCtrl.GetValue()
            self.dbm = mongoDBM.DBManager(uri, '', '')
        except Exception:
            self.ReportMsg("Connnect MongoDB failed: {}.".format(uri), "Connect Fail")
        if self.dbm.client is not None:
            self.ReportMsg("Connnect MongoDB successfully: {}.".format(uri), "Connect Success",)
        else:
            self.ReportMsg("Connnect MongoDB failed: {}.".format(uri), "Connect Fail")


    def OnSwitch(self, event):
        try:
            db = self.mongo_db_TextCtrl.GetValue()
            coll = self.mongo_coll_TextCtrl.GetValue()
            self.dbm.db_name = db
            self.dbm.coll_name = coll
        except Exception:
            self.ReportMsg("Switch failed, please check your database or collection.", "Switch failed")

        if self.dbm.coll is not None:
            self.db = db
            self.coll = coll
            msg = "Switch successfully: now database is {}, collection is {}.".format(db, coll)
            self.ReportMsg(msg, "Switch Success")
        else:
            msg = "Switch failed, please check your database or collection."
            self.ReportMsg(msg, msg)


    def OnSchemaAnalyser(self, event):
        self.SetStatusText("Wait a moment, Analysing...")
        self.list.DeleteAllItems()

        uri = self.mongo_uri_TextCtrl.GetValue()
        db = self.mongo_db_TextCtrl.GetValue()
        coll = self.mongo_coll_TextCtrl.GetValue()

        try:
            query_str = self.query_TextCtrl.GetValue()
            if query_str == "":
                query = {}
            else:
                query = eval(query_str)
                # Or using json.loads(inputStr) ->  bson.json_util.loads(json) -> bson ?

            order_int = self.order_Choice.GetSelection()
            if order_int != 0:
                order = -1
            else:
                order = 1

            limit_str = self.limit_TextCtrl.GetValue()
            if limit_str == "":
                limit = 0
            else:
                limit = int(limit_str)

            omit_keys_str = self.omit_keys_TextCtrl.GetValue()  # comma separate
            omit_keys = []
            if omit_keys_str != "":
                for i in omit_keys_str.split(","):
                    omit_keys.append(i.strip())
            omit_patterns_str = self.omit_patterns_TextCtrl.GetValue()
            omit_patterns = []
            if omit_patterns_str != "":
                for j in omit_patterns_str.split(","):
                    omit_patterns.append(j.strip())
                
            embed_bool = self.embed_CheckBox.GetValue()
            if embed_bool:
                embed = "yes"
            else:
                embed = "no"
        except Exception as e:
            self.ReportMsg(u"Get query, limit, omit_keys or omit_patterns failed.",
                           u"Get query, limit, omit_keys or omit_patterns failed.")

        # MapReduce 
        # The mongo shell treats all numbers as 64-bit floating-point double values by default.
        # The mongo shell provides the NumberInt() constructor to explicitly specify 32-bit integers.
        # The mongo shell provides the NumberLong() wrapper to handle 64-bit integers.
        # The mongo shell provides the NumberDecimal() constructor to explicitly specify 128-bit decimal-based floating-point values
        # For NosqlBooster js shell, make integer n (n <= 2147483647 && n >= -2147483648) as Double automatically.
        from bson.code import Code
        mapper = Code('''
        function () {
            function analyse(Object_BSON, prefix=""){
                BSON_LOOP:
                for (var i in Object_BSON){
                    var n = Object_BSON[i];

                    if (OMIT_KEYS.indexOf(i) > -1) { continue; }
                    for (var j in OMIT_PATTERNS) {
                        var patt = new RegExp(OMIT_PATTERNS[j]);
                        if (patt.test(i)) { continue BSON_LOOP; }
                    }

                    var type = toString.call(n).slice(8, -1);
                    if (type === "Number") {
                        if (n % 1 === 0 && n <= 2147483647 && n >= -2147483648) {
                            type = "Int32";
                        } else {
                            type = "Double";
                        }
                    }
                    if (prefix === "") {
                        compoundKey = i;
                    } else {
                        compoundKey = prefix + '.' + i;
                    }                    
                    var emitKey = compoundKey + "##" + type;
                    emit(emitKey, 1);

                    if (EMBED === "yes") {
                        if (type === "BSON") {
                            analyse(n, compoundKey);
                        }
                    }
                }
            }

            analyse(this)           
        }
        ''')
        reducer = Code('''
        function (key, values) {
            var total = 0;
            for (var i=0; i < values.length; i++) {
                total += values[i];
            }
            return total;
        }
        ''')
        # other parameters to control the result
        # See https://docs.mongodb.com/manual/reference/command/mapReduce/#dbcmd.mapReduce
        query = query
        sort = {"_id": order}
        limit = limit
        scope = {'OMIT_KEYS': omit_keys, 'OMIT_PATTERNS': omit_patterns, 'EMBED': embed}
        
        coll_stat = self.dbm.db.command("collstats", self.dbm.coll_name)
        if coll_stat.get("sharded") == False:
            rstJsonList = self.dbm.coll.inline_map_reduce(mapper, reducer, query=query, sort=sort, limit=limit, scope=scope)
        else:
            rstJsonList = self.dbm.coll.inline_map_reduce(mapper, reducer, query=query, sort=sort, scope=scope)

        total = 1
        key_dict = {}
        for doc in rstJsonList:
            id = doc['_id']
            idArr = id.split("##")
            key = idArr[0]
            type = idArr[1]
            value = doc['value']
            if key == "_id":
                total = value
            key_all = key_dict.get(key, None)
            if key_all is None:
                key_dict[key] = {'key': key, 'total_occurrence': float(value),
                                'statics': [{'type': type, 'occurrence': value, 'percent': 0.0}]}
            else:
                key_dict[key]['total_occurrence'] = float(key_all['total_occurrence']) + float(value)
                key_dict[key]['statics'].append({'type': type, 'occurrence': value, 'percent': 0.0})
        rstMapList = []
        for i in key_dict.keys():
            doc = key_dict[i]
            for j in range(len(doc['statics'])):
                doc['statics'][j]['percent'] = float(doc['statics'][j]['occurrence']) * 100 / float(total)
            rstMapList.append(doc)

        # sort 
        sortedRstMapList = sorted(rstMapList, key=lambda x: (-x['total_occurrence'], x['key']))

        # json dumps json to strings
        self.rst_write_str = json.dumps(sortedRstMapList, ensure_ascii=False, indent=4)


        # to listCtrl
        index = 0
        for rstMap in sortedRstMapList:
            index = self.list.InsertItem(self.list.GetItemCount(), str(index))
            self.list.SetItem(index, 1, str(rstMap["key"])) 
            self.list.SetItem(index, 2, str(rstMap["total_occurrence"])) 
            staticStr = ""
            for i in rstMap['statics']:
                staticStr += "{}({}-{}%) ".format(i['type'], i['occurrence'], i['percent'])
            self.list.SetItem(index, 3, staticStr)  
            index += 1
        msg = "Schema Analyse Done, database {}, collection {}".format(db, coll)
        self.SetStatusText(msg)

    def save_file_content(self, event):

            self.dir_name = ''
            fd = wx.FileDialog(self, 'Save to Json file...', self.dir_name, '{}_{}-Schema.json'.format(self.db, self.coll), 'JSON file(*.json)|*.json', wx.FD_SAVE)
            if fd.ShowModal() == wx.ID_OK:
                self.file_name = fd.GetFilename()
                self.dir_name = fd.GetDirectory()
                try:
                    with open(os.path.join(self.dir_name, self.file_name), 'w', encoding='utf-8') as f:
                        text = self.rst_write_str
                        f.write(text)
                        save_msg = wx.MessageDialog(self, 'Save Successfully', 'Tip')
                except FileNotFoundError:
                    save_msg = wx.MessageDialog(self, 'Save failed', 'Tip')    
            else:
                save_msg = wx.MessageDialog(self, 'Select no directory', 'Error')

            save_msg.ShowModal()
            save_msg.Destroy()


if __name__ == '__main__':

    # When this module is run (not imported) then create the app, the
    # frame, show it, and start the event loop.
    app = wx.App()
    frm = PyMonSchemaFrame(None, title='Schema Analyser')
    frm.Show()
    app.MainLoop()
