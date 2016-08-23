package DataCaseClasses

// case for {"love":1,"haha":0,"like":5,"sorry":0,"anger":0,"wow":0}
case class Reaction(love: Int, haha: Int, like: Int, sorry: Int, anger: Int, wow: Int) extends Serializable {
        def -(that: Reaction) = new Reaction(
            this.love - that.love,
            this.haha - that.haha,
            this.like - that.like,
            this.sorry - that.sorry,
            this.anger - that.anger,
            this.wow - that.wow)
        override def toString = """{"love":""" + love + ""","haha":""" + haha + ""","like":""" + like + ""","sorry":""" + sorry + ""","anger":""" + anger + ""","wow":""" + wow + "}"
}

// case for {"like":47,"share":5,"comment":5}
case class Action(like: Int, share: Int, comment: Int) extends Serializable {
        def -(that: Action) = new Action(
            this.like - that.like,
            this.share - that.share,
            this.comment - that.comment)
        override def toString = """{"like":""" + like + ""","share":""" + share + ""","comment":""" + comment + "}"	
}

case class Stats(description: String, title: String, name: String, id: String, obj_type: String, period: String, stats_type: String, sys_time: String, old_value: String, new_value: String, value: String) extends Serializable {
    	override def toString = "{" + """"sys_time":"""" + sys_time + """","title":"""" + title + """","name":"""" + name  + """","id":"""" + id + """","obj_type":"""" + obj_type + """","period":"""" + period + """","stats_type":"""" + stats_type + """","old_value:":""" + old_value + ""","new_value":""" + new_value + ""","value":""" + value + "}"

}

