package bigdata;

public class Value {
    public float shiftX;
    public float shiftY;

    public Value(float shiftX, float shiftY){
        this.shiftX = shiftX;
        this.shiftY = shiftY;
    }

    public boolean equals(Object o){
        if(!(o instanceof Value)){
            return false;
        }
        Value vo = (Value) o;
        return vo.shiftX == this.shiftX && vo.shiftY == this.shiftY;
    }
}
