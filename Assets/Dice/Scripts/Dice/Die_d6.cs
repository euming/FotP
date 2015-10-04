/**
 * Copyright (c) 2010-2015, WyrmTale Games and Game Components
 * All rights reserved.
 * http://www.wyrmtale.com
 *
 * THIS SOFTWARE IS PROVIDED BY WYRMTALE GAMES AND GAME COMPONENTS 'AS IS' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL WYRMTALE GAMES AND GAME COMPONENTS BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR 
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */ 
using UnityEngine;
using System.Collections;

// Die subclass to expose the D6 side hitVectors
public class Die_d6 : Die {

	public float[] dotValues = new float[6];

    override protected Vector3 HitVector(int side)
    {
        switch (side)
        {
            case 1: return new Vector3(0F, 0F, 1F);
            case 2: return new Vector3(0F, -1F, 0F);
            case 3: return new Vector3(-1F, 0F, 0F);
            case 4: return new Vector3(1F, 0F, 0F);
            case 5: return new Vector3(0F, 1F, 0F);
            case 6: return new Vector3(0F, 0F, -1F);
        }
        return Vector3.zero;
    }
	public void AlignTowardsVector(Vector3 curVector, Vector3 destVector)
	{
		//Vector3 curUp = GetCurUpVector(this.value);
		float dotProduct = Vector3.Dot (curVector, destVector);	//	near 1.0f or 1.0f
		float tolerance = 0.0000f;
		if (dotProduct > 1.0f)
			dotProduct = 1.0f;
		float angleRadians = Mathf.Acos (dotProduct);
		float angleDegrees = Mathf.Rad2Deg * angleRadians;
		if ( dotProduct < 1.0f - tolerance) {
			Vector3 perpendicular = Vector3.Cross (curVector, destVector);
			perpendicular = Vector3.Normalize (perpendicular);
			this.transform.RotateAround (this.transform.position, perpendicular, angleDegrees);
		}
		//Debug.Log ("Rotate by " + angleDegrees.ToString ());
	}

	//	orient towards up.	point directly to the side that's already up.
	public void OrientTowardsUp()
	{
		Vector3 curUp = GetCurUpVector(this.GetSide());
		AlignTowardsVector (curUp, Vector3.up);
	}

	//	assuming already up, orient so 0/90/180/270 degrees
	public void OrientTowardStraightened()
	{
		Vector3 curSide = GetCurSideVector (this.GetSide());
		AlignTowardsVector (curSide, Vector3.right);
	}

	public void OrientTowardLinedUp()
	{
		OrientTowardsUp ();
		OrientTowardStraightened ();
	}
	public Vector3 GetCurSideVector(int side_value)
	{
		Vector3 side_vector = new Vector3(0,0,0);
		switch (side_value) {
		default:
			break;
		case 1:
			side_vector = (this.transform.right);
			break;
		case 2:
			side_vector = -(this.transform.forward);
			break;
		case 3:
			side_vector = (-this.transform.up);
			break;
		case 4:
			side_vector = (this.transform.up);
			break;
		case 5:
			side_vector = (this.transform.forward);
			break;
		case 6:
			side_vector = (-this.transform.right);
			break;
		}
		return side_vector;
	}

	public Vector3 GetCurUpVector(int side_value)
	{
		Vector3 up_vector = new Vector3(0,0,0);
		switch (side_value) {
		default:
			break;
		case 1:
			up_vector = (this.transform.forward);
			break;
		case 2:
			up_vector = -(this.transform.up);
			break;
		case 3:
			up_vector = (-this.transform.right);
			break;
		case 4:
			up_vector = (this.transform.right);
			break;
		case 5:
			up_vector = (this.transform.up);
			break;
		case 6:
			up_vector = (-this.transform.forward);
			break;
		}
		return up_vector;
	}

	//	get the side that's up
	override public int GetSide()
	{
		dotValues[0] = Vector3.Dot(this.transform.forward, Vector3.up);
		dotValues[1] = Vector3.Dot (-this.transform.up, Vector3.up);
		dotValues[2] = Vector3.Dot(-this.transform.right, Vector3.up);
		dotValues[3] = Vector3.Dot (this.transform.right, Vector3.up);
		dotValues[4] = Vector3.Dot(this.transform.up, Vector3.up);
		dotValues[5] = Vector3.Dot (-this.transform.forward, Vector3.up);

		float bestDot = 0.0f;
		int bestSide = 0;
		for(int jj=0; jj<6; jj++) {
			if (dotValues[jj] > bestDot) {
				bestDot = dotValues[jj];
				bestSide = jj;
			}
		}
		return bestSide+1;
	}
}
